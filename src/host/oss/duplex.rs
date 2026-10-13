use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use super::{
    ioctl, map_io_error,
    stream::{DuplexDataCallback, DuplexStreamInner, ErrorCallback, arm_trigger},
    trigger::TriggerReceiver,
    worker::{check_xrun, pack_24, unpack_24},
};
use crate::{
    CallbackInfo, Data, DuplexCallbackInfo, Error, ErrorKind, FrameCount, StreamInstant,
    StreamTimestamp, host::frames_to_duration,
};

/// Starts a duplex stream: pre-fills playback with silence and triggers both directions.
///
/// Without the pre-fill, triggering playback on an empty buffer underruns immediately and
/// capture/playback start misaligned.
pub(super) fn begin_duplex_playback(stream: &DuplexStreamInner) -> Result<(), Error> {
    let sample_format = stream.playback.sample_format;
    let mut silence =
        vec![0u8; stream.playback.period_samples * sample_format.sample_size()].into_boxed_slice();
    stream.playback_equilibrium.fill(&mut silence);

    // Packed 24-bit: equilibrium is computed in container layout, repacked for the wire.
    let wire;
    let silence: &[u8] = if stream.packed_24 {
        let mut packed = vec![0u8; stream.period_size * stream.frame_size].into_boxed_slice();
        pack_24(&silence, &mut packed, sample_format);
        wire = packed;
        &wire
    } else {
        &silence
    };

    for _ in 0..super::DEFAULT_PERIODS {
        write_all(stream.fd.0, silence)?;
    }

    arm_trigger(
        stream.fd.0,
        stream.caps,
        &stream.triggered,
        ioctl::PCM_ENABLE_INPUT | ioctl::PCM_ENABLE_OUTPUT,
    )
}

// Writes a full buffer to the non-blocking fd, polling for writability on EAGAIN.
fn write_all(fd: libc::c_int, buf: &[u8]) -> Result<(), Error> {
    let mut written = 0;
    while written < buf.len() {
        let n = unsafe { libc::write(fd, buf[written..].as_ptr().cast(), buf.len() - written) };
        if n > 0 {
            written += n as usize;
            continue;
        }
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) => {
                let mut pollfd = libc::pollfd {
                    fd,
                    events: libc::POLLOUT,
                    revents: 0,
                };
                // SAFETY: `pollfd` is a valid 1-element array.
                let res = unsafe { libc::poll(&mut pollfd, 1, -1) };
                if res < 0 {
                    let poll_err = std::io::Error::last_os_error();
                    if poll_err.raw_os_error() != Some(libc::EINTR) {
                        return Err(map_io_error(poll_err));
                    }
                }
            }
            _ => return Err(map_io_error(err)),
        }
    }
    Ok(())
}

pub(super) fn duplex_stream_worker(
    rx: Arc<TriggerReceiver>,
    stream: &DuplexStreamInner,
    data_callback: &mut DuplexDataCallback,
    error_callback: &mut ErrorCallback,
    timeout: Option<Duration>,
) {
    let mut ctxt = DuplexWorkerContext::new(&timeout, stream, &rx);
    loop {
        if stream.control.dropping.load(Ordering::Relaxed) {
            return;
        }
        if stream.control.parked.load(Ordering::Relaxed) {
            stream.control.acknowledge_park();
        }
        let result = match poll_for_duplex_period(&rx, stream, &mut ctxt) {
            Ok(true) => process_duplex(stream, &mut ctxt, data_callback),
            Ok(false) => Ok(()),
            Err(err) => Err(err),
        };
        if let Err(err) = result {
            match err.kind() {
                ErrorKind::DeviceNotAvailable => {
                    error_callback(err);
                    stream.control.signal_worker_exit();
                    return;
                }
                _ => error_callback(err),
            }
        }
    }
}

struct DuplexWorkerContext {
    poll_fds: [libc::pollfd; 2],
    /// Wire-layout transfer buffers, as read/written on the fd.
    capture_wire: Box<[u8]>,
    playback_wire: Box<[u8]>,
    /// Container-layout callback buffers for packed 24-bit formats; when present, the
    /// worker converts between these and the wire buffers around the callback.
    capture_samples: Option<Box<[u8]>>,
    playback_samples: Option<Box<[u8]>>,
    poll_timeout: i32,
}

impl DuplexWorkerContext {
    fn new(
        poll_timeout: &Option<Duration>,
        stream: &DuplexStreamInner,
        rx: &TriggerReceiver,
    ) -> Self {
        let poll_timeout = super::poll_timeout_millis(*poll_timeout);

        let capture_wire =
            vec![0u8; stream.period_size * stream.capture.frame_size].into_boxed_slice();
        let playback_wire =
            vec![0u8; stream.period_size * stream.playback.frame_size].into_boxed_slice();
        let sample_bytes =
            stream.capture.period_samples * stream.capture.sample_format.sample_size();
        let (capture_samples, playback_samples) = if stream.packed_24 {
            (
                Some(vec![0u8; sample_bytes].into_boxed_slice()),
                Some(vec![0u8; sample_bytes].into_boxed_slice()),
            )
        } else {
            (None, None)
        };

        let poll_fds = [
            libc::pollfd {
                fd: rx.0,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: stream.fd.0,
                events: libc::POLLIN | libc::POLLOUT,
                revents: 0,
            },
        ];

        Self {
            poll_fds,
            capture_wire,
            playback_wire,
            capture_samples,
            playback_samples,
            poll_timeout,
        }
    }
}

// Neither direction is processed until a full capture period is available AND a full
// playback period fits, keeping capture and playback in lockstep on the shared clock.
fn poll_for_duplex_period(
    rx: &TriggerReceiver,
    stream: &DuplexStreamInner,
    ctxt: &mut DuplexWorkerContext,
) -> Result<bool, Error> {
    let res = unsafe { libc::poll(ctxt.poll_fds.as_mut_ptr(), 2, ctxt.poll_timeout) };
    if res < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            return Ok(false);
        }
        return Err(map_io_error(err));
    }
    if res == 0 {
        return Ok(false);
    }

    if ctxt.poll_fds[0].revents != 0 {
        rx.clear_pipe();
        return Ok(false);
    }

    let revents = ctxt.poll_fds[1].revents;
    if revents & (libc::POLLHUP | libc::POLLNVAL) != 0 {
        return Err(Error::with_message(
            ErrorKind::DeviceNotAvailable,
            "Device disconnected",
        ));
    }

    check_xrun(
        stream.fd.0,
        &stream.last_play_underruns,
        &stream.last_rec_overruns,
        &stream.pending_xrun,
    );

    if revents & (libc::POLLIN | libc::POLLOUT) == 0 {
        return Ok(false);
    }

    let capture_ready = ioctl::get_ispace(stream.fd.0)
        .map(|info| info.bytes as usize >= ctxt.capture_wire.len())
        .unwrap_or(false);
    let playback_ready = ioctl::get_ospace(stream.fd.0)
        .map(|info| info.bytes as usize >= ctxt.playback_wire.len())
        .unwrap_or(false);

    Ok(capture_ready && playback_ready)
}

// Invokes the duplex data callback with container-layout buffers. `input` and `output`
// are in cpal container layout (4-byte stride for I24/U24).
fn invoke_duplex_callback(
    stream: &DuplexStreamInner,
    input: &[u8],
    output: &mut [u8],
    data_callback: &mut DuplexDataCallback,
) {
    let input_ptr = input.as_ptr() as *mut ();
    let input_data = unsafe {
        Data::from_parts(
            input_ptr,
            stream.capture.period_samples,
            stream.capture.sample_format,
        )
    };
    let output_ptr = output.as_mut_ptr() as *mut ();
    let mut output_data = unsafe {
        Data::from_parts(
            output_ptr,
            stream.playback.period_samples,
            stream.playback.sample_format,
        )
    };

    let now = std::time::Instant::now();
    let d = now.duration_since(stream.creation_instant);
    let callback = StreamInstant::new(d.as_secs(), d.subsec_nanos());
    let delay_frames =
        (ioctl::get_odelay(stream.fd.0).unwrap_or(0).max(0) as usize) / stream.playback.frame_size;
    let delay = frames_to_duration(delay_frames as FrameCount, stream.sample_rate);

    let xrun = stream.pending_xrun.swap(false, Ordering::Relaxed);
    let info = DuplexCallbackInfo::new(
        CallbackInfo {
            timestamp: StreamTimestamp {
                callback,
                device: callback,
            },
            xrun,
        },
        CallbackInfo {
            timestamp: StreamTimestamp {
                callback,
                device: callback + delay,
            },
            xrun,
        },
    );
    data_callback(&input_data, &mut output_data, &info);
}

fn process_duplex(
    stream: &DuplexStreamInner,
    ctxt: &mut DuplexWorkerContext,
    data_callback: &mut DuplexDataCallback,
) -> Result<(), Error> {
    let DuplexWorkerContext {
        capture_wire,
        playback_wire,
        capture_samples,
        playback_samples,
        ..
    } = ctxt;

    let capture_bytes = capture_wire.len();
    let mut read_bytes = 0;
    while read_bytes < capture_bytes {
        let n = unsafe {
            libc::read(
                stream.fd.0,
                capture_wire[read_bytes..].as_mut_ptr().cast(),
                capture_bytes - read_bytes,
            )
        };
        if n > 0 {
            read_bytes += n as usize;
            continue;
        }
        if n == 0 {
            continue;
        }
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) => return Ok(()),
            _ => return Err(map_io_error(err)),
        }
    }

    // Expand the packed wire layout into container layout for the callback.
    let capture_container: &mut [u8] = match capture_samples {
        Some(samples) => {
            unpack_24(capture_wire, samples, stream.capture.sample_format);
            samples
        }
        None => capture_wire,
    };

    // The callback fills the container layout; repack to the wire after.
    let playback_container: &mut [u8] = match playback_samples {
        Some(samples) => samples,
        None => playback_wire,
    };

    stream.playback_equilibrium.fill(playback_container);

    if !stream.control.draining.load(Ordering::Relaxed) {
        invoke_duplex_callback(stream, capture_container, playback_container, data_callback);
    }

    if let Some(samples) = playback_samples {
        pack_24(samples, playback_wire, stream.playback.sample_format);
    }

    let playback_bytes = playback_wire.len();
    let mut written_bytes = 0;
    while written_bytes < playback_bytes {
        let n = unsafe {
            libc::write(
                stream.fd.0,
                playback_wire[written_bytes..].as_ptr().cast(),
                playback_bytes - written_bytes,
            )
        };
        if n > 0 {
            written_bytes += n as usize;
            continue;
        }
        if n == 0 {
            continue;
        }
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) => return Ok(()),
            _ => return Err(map_io_error(err)),
        }
    }
    Ok(())
}
