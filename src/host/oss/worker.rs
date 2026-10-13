use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use super::{
    ioctl, map_io_error,
    stream::{ErrorCallback, InputDataCallback, OutputDataCallback, StreamInner},
    trigger::TriggerReceiver,
};
use crate::{
    CallbackInfo, Data, Error, ErrorKind, FrameCount, SampleFormat, StreamInstant, StreamTimestamp,
    host::frames_to_duration,
};

// The backend only negotiates AFMT_*_NE formats, so the wire is always native-endian.
const WIRE_BIG_ENDIAN: bool = cfg!(target_endian = "big");

/// Repacks 4-byte-container I24/U24 samples into 3-byte-packed wire samples.
///
/// `container` holds `packed.len() / 3` samples in cpal's native-endian container layout;
/// U24's `1 << 23` origin is rebased to signed two's complement.
pub(super) fn pack_24(container: &[u8], packed: &mut [u8], format: SampleFormat) {
    debug_assert_eq!(container.len(), packed.len() / 3 * 4);
    for (i, chunk) in packed.chunks_exact_mut(3).enumerate() {
        let bits = u32::from_ne_bytes(container[i * 4..i * 4 + 4].try_into().unwrap());
        let signed = match format {
            SampleFormat::U24 => (bits as i32).wrapping_sub(1 << 23) as u32,
            _ => bits,
        };
        let bytes = if WIRE_BIG_ENDIAN {
            // Emit the low 24 bits most-significant first.
            signed.wrapping_shl(8).to_be_bytes()
        } else {
            signed.to_le_bytes()
        };
        chunk.copy_from_slice(&bytes[..3]);
    }
}

/// Expands 3-byte-packed wire samples into 4-byte-container I24/U24 samples.
///
/// Sign-extends the packed 24-bit two's complement value into the container; U24 is
/// rebased by its `1 << 23` origin.
pub(super) fn unpack_24(packed: &[u8], container: &mut [u8], format: SampleFormat) {
    debug_assert_eq!(container.len(), packed.len() / 3 * 4);
    for (i, chunk) in packed.chunks_exact(3).enumerate() {
        let bits: u32 = if WIRE_BIG_ENDIAN {
            (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2])
        } else {
            u32::from(chunk[0]) | (u32::from(chunk[1]) << 8) | (u32::from(chunk[2]) << 16)
        };
        // Shift the sign bit into place and sign-extend it back out.
        let signed = ((bits << 8) as i32) >> 8;
        let container_bits = match format {
            SampleFormat::U24 => (signed + (1 << 23)) as u32,
            _ => signed as u32,
        };
        container[i * 4..i * 4 + 4].copy_from_slice(&container_bits.to_ne_bytes());
    }
}

pub(super) fn input_stream_worker(
    rx: Arc<TriggerReceiver>,
    stream: &StreamInner,
    data_callback: &mut InputDataCallback,
    error_callback: &mut ErrorCallback,
    timeout: Option<Duration>,
) {
    let mut ctxt = StreamWorkerContext::new(&timeout, stream, &rx, libc::POLLIN);
    loop {
        if stream.control.dropping.load(Ordering::Relaxed) {
            return;
        }
        if stream.control.parked.load(Ordering::Relaxed) {
            stream.control.acknowledge_park();
        }
        let result = match poll_for_period(&rx, stream, &mut ctxt) {
            Ok(true) => process_input(stream, &mut ctxt, data_callback),
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

pub(super) fn output_stream_worker(
    rx: Arc<TriggerReceiver>,
    stream: &StreamInner,
    data_callback: &mut OutputDataCallback,
    error_callback: &mut ErrorCallback,
    timeout: Option<Duration>,
) {
    let mut ctxt = StreamWorkerContext::new(&timeout, stream, &rx, libc::POLLOUT);
    loop {
        if stream.control.dropping.load(Ordering::Relaxed) {
            return;
        }
        if stream.control.parked.load(Ordering::Relaxed) {
            stream.control.acknowledge_park();
        }
        let result = match poll_for_period(&rx, stream, &mut ctxt) {
            Ok(true) => process_output(stream, &mut ctxt, data_callback),
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

struct StreamWorkerContext {
    poll_fds: [libc::pollfd; 2],
    /// Transfer buffer as read/written on the wire.
    wire_buffer: Box<[u8]>,
    /// Callback-facing buffer in cpal container layout; only allocated for packed 24-bit
    /// formats, where the wire and container layouts differ. Otherwise the callback uses
    /// `wire_buffer` directly.
    sample_buffer: Option<Box<[u8]>>,
    poll_timeout: i32,
}

impl StreamWorkerContext {
    fn new(
        poll_timeout: &Option<Duration>,
        stream: &StreamInner,
        rx: &TriggerReceiver,
        dsp_events: libc::c_short,
    ) -> Self {
        let poll_timeout = super::poll_timeout_millis(*poll_timeout);

        // Pre-allocate period-sized working buffers. Contents are overwritten each callback.
        let wire_buffer = vec![0u8; stream.period_size * stream.frame_size].into_boxed_slice();
        let sample_buffer = stream.packed_24.then(|| {
            vec![0u8; stream.period_samples * stream.sample_format.sample_size()].into_boxed_slice()
        });

        let poll_fds = [
            libc::pollfd {
                fd: rx.0,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: stream.fd.0,
                events: dsp_events,
                revents: 0,
            },
        ];

        Self {
            poll_fds,
            wire_buffer,
            sample_buffer,
            poll_timeout,
        }
    }
}

/// Polls the trigger pipe and the dsp fd. Returns `true` when the dsp fd reports the
/// requested event; the caller must still verify a full period of space/data is available.
fn poll_for_period(
    rx: &TriggerReceiver,
    stream: &StreamInner,
    ctxt: &mut StreamWorkerContext,
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
        // Self-pipe fired: the stream is being dropped, paused, or stopped. Clear the pipe
        // and let the worker loop observe the new state on the next iteration.
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
    // POLLERR accompanies an over/underrun on some drivers; OSS recovers automatically and
    // the xrun counters report it below.

    check_xrun(
        stream.fd.0,
        &stream.last_play_underruns,
        &stream.last_rec_overruns,
        &stream.pending_xrun,
    );

    Ok(revents & ctxt.poll_fds[1].events != 0)
}

/// Detects new over/underruns via SNDCTL_DSP_GETERROR counter deltas. OSS recovers from
/// xruns by itself; this only flags the next callback to report one.
pub(super) fn check_xrun(
    fd: libc::c_int,
    last_play_underruns: &std::sync::atomic::AtomicI32,
    last_rec_overruns: &std::sync::atomic::AtomicI32,
    pending_xrun: &std::sync::atomic::AtomicBool,
) {
    if let Ok(info) = ioctl::get_error(fd) {
        let underruns = last_play_underruns.load(Ordering::Relaxed);
        let overruns = last_rec_overruns.load(Ordering::Relaxed);
        if info.play_underruns > underruns || info.rec_overruns > overruns {
            pending_xrun.store(true, Ordering::Relaxed);
        }
        last_play_underruns.store(info.play_underruns.max(underruns), Ordering::Relaxed);
        last_rec_overruns.store(info.rec_overruns.max(overruns), Ordering::Relaxed);
    }
}

// Read input data from the device and deliver it to the user.
fn process_input(
    stream: &StreamInner,
    ctxt: &mut StreamWorkerContext,
    data_callback: &mut InputDataCallback,
) -> Result<(), Error> {
    let StreamWorkerContext {
        wire_buffer,
        sample_buffer,
        ..
    } = ctxt;

    let wire_len = wire_buffer.len();
    let mut read_bytes = 0;
    while read_bytes < wire_len {
        let n = unsafe {
            libc::read(
                stream.fd.0,
                wire_buffer[read_bytes..].as_mut_ptr().cast(),
                wire_len - read_bytes,
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
            // No frames available or a partially read period: skip this cycle. The period
            // is re-read in full next time; OSS has no prepare() to re-arm.
            Some(libc::EAGAIN) => return Ok(()),
            _ => return Err(map_io_error(err)),
        }
    }

    // Packed 24-bit: expand the wire layout into the container layout for the callback.
    let samples: &mut [u8] = match sample_buffer {
        Some(samples) => {
            unpack_24(wire_buffer, samples, stream.sample_format);
            samples
        }
        None => wire_buffer,
    };

    if !stream.control.draining.load(Ordering::Relaxed) {
        let data = samples.as_mut_ptr() as *mut ();
        let data = unsafe { Data::from_parts(data, stream.period_samples, stream.sample_format) };
        // No capture delay estimate is available from OSS; the device timestamp is the
        // callback instant, kept monotonic by the caller-side wrapper.
        let callback = callback_instant(stream);
        let timestamp = StreamTimestamp {
            callback,
            device: callback,
        };
        let xrun = stream.pending_xrun.swap(false, Ordering::Relaxed);
        data_callback(&data, &CallbackInfo { timestamp, xrun });
    }

    Ok(())
}

/// The stream clock origin shared with `StreamTrait::now`: elapsed time since creation.
fn callback_instant(stream: &StreamInner) -> StreamInstant {
    let d = std::time::Instant::now().duration_since(stream.creation_instant);
    StreamInstant::new(d.as_secs(), d.subsec_nanos())
}

// Request data from the user's function and write it to the device.
fn process_output(
    stream: &StreamInner,
    ctxt: &mut StreamWorkerContext,
    data_callback: &mut OutputDataCallback,
) -> Result<(), Error> {
    let StreamWorkerContext {
        wire_buffer,
        sample_buffer,
        ..
    } = ctxt;

    // The callback fills the container layout; packed 24-bit is repacked to the wire after.
    match sample_buffer {
        Some(samples) => {
            fill_and_invoke_output(stream, samples, data_callback);
            pack_24(samples, wire_buffer, stream.sample_format);
        }
        None => fill_and_invoke_output(stream, wire_buffer, data_callback),
    }

    let wire_len = wire_buffer.len();
    let mut written_bytes = 0;
    while written_bytes < wire_len {
        let n = unsafe {
            libc::write(
                stream.fd.0,
                wire_buffer[written_bytes..].as_ptr().cast(),
                wire_len - written_bytes,
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
            // Buffer full or a partially written period: skip this cycle. OSS plays out
            // what was accepted and recovers from the underrun by itself.
            Some(libc::EAGAIN) => return Ok(()),
            _ => return Err(map_io_error(err)),
        }
    }
    Ok(())
}

// Pre-fills the buffer with equilibrium, then requests a period of output data from the
// user. `samples` is in cpal container layout (4-byte stride for I24/U24).
fn fill_and_invoke_output(
    stream: &StreamInner,
    samples: &mut [u8],
    data_callback: &mut OutputDataCallback,
) {
    // Pre-fill buffer with equilibrium; user callback overwrites what it wants.
    stream
        .equilibrium
        .as_ref()
        .expect("fill_and_invoke_output only runs for Output-direction streams")
        .fill(samples);

    if !stream.control.draining.load(Ordering::Relaxed) {
        let data = samples.as_mut_ptr() as *mut ();
        let mut data =
            unsafe { Data::from_parts(data, stream.period_samples, stream.sample_format) };
        let callback = callback_instant(stream);
        let delay_frames =
            (ioctl::get_odelay(stream.fd.0).unwrap_or(0).max(0) as usize) / stream.frame_size;
        let delay = frames_to_duration(delay_frames as FrameCount, stream.sample_rate);
        let timestamp = StreamTimestamp {
            callback,
            device: callback + delay,
        };
        let xrun = stream.pending_xrun.swap(false, Ordering::Relaxed);
        data_callback(&mut data, &CallbackInfo { timestamp, xrun });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{I24, U24};

    fn container_bytes(samples: &[i32]) -> Vec<u8> {
        samples.iter().flat_map(|s| s.to_ne_bytes()).collect()
    }

    #[test]
    fn pack_unpack_i24_round_trips() {
        // Covers zero, positive, negative, and both 24-bit extremes.
        let values = [0i32, 1, -1, 42, -42, 8_388_607, -8_388_608];
        let container = container_bytes(&values);
        let mut packed = vec![0u8; values.len() * 3];
        pack_24(&container, &mut packed, SampleFormat::I24);

        let mut round_tripped = vec![0u8; container.len()];
        unpack_24(&packed, &mut round_tripped, SampleFormat::I24);
        assert_eq!(container, round_tripped);

        // Each packed sample is the low 24 bits of the container value.
        for (i, value) in values.iter().enumerate() {
            assert_eq!(
                packed[i * 3..i * 3 + 3],
                value.to_le_bytes()[..3],
                "packed sample {i}"
            );
        }
    }

    #[test]
    fn pack_unpack_u24_rebases_origin() {
        // Zero signal sits at the 1 << 23 origin; extremes stay in range.
        let values = [0u32, 1, (1 << 23) - 1, 1 << 23, (1 << 24) - 1];
        let container: Vec<u8> = values.iter().flat_map(|s| s.to_ne_bytes()).collect();
        let mut packed = vec![0u8; values.len() * 3];
        pack_24(&container, &mut packed, SampleFormat::U24);

        let mut round_tripped = vec![0u8; container.len()];
        unpack_24(&packed, &mut round_tripped, SampleFormat::U24);
        assert_eq!(container, round_tripped);

        // The 1 << 23 origin (container value at index 3) maps to silence on the signed
        // wire; max U24 maps to the top of two's complement range.
        assert_eq!(packed[9..12], [0, 0, 0]);
        assert_eq!(
            packed[12..15],
            ((1u32 << 23) - 1).to_le_bytes()[..3],
            "max U24 maps to max positive signed 24-bit"
        );
        // Container value 0 (most negative signal) maps to min signed 24-bit.
        assert_eq!(packed[0..3], [0x00, 0x00, 0x80]);
    }

    #[test]
    fn unpack_i24_sign_extends() {
        // 0xFFFFFF on the wire is -1: the container must sign-extend, not zero-extend.
        let packed = [0xffu8, 0xff, 0xff, 0x00, 0x00, 0x80];
        let mut container = vec![0u8; 8];
        unpack_24(&packed, &mut container, SampleFormat::I24);
        assert_eq!(i32::from_ne_bytes(container[0..4].try_into().unwrap()), -1);
        assert_eq!(
            i32::from_ne_bytes(container[4..8].try_into().unwrap()),
            i32::MIN >> 8
        );

        // The results are valid dasp 24-bit sample values.
        assert_eq!(
            I24::new(i32::from_ne_bytes(container[0..4].try_into().unwrap())),
            I24::new(-1)
        );
        let _ = U24::new(0); // U24 is exercised by the origin test above.
    }
}
