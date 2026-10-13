use std::{
    os::fd::RawFd,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use super::{
    duplex::{begin_duplex_playback, duplex_stream_worker},
    ioctl, map_io_error,
    trigger::{TriggerReceiver, TriggerSender, trigger},
    worker::{input_stream_worker, output_stream_worker},
};
use crate::{
    CallbackInfo, Data, DeviceDirection, DuplexCallbackInfo, Error, FrameCount, SampleFormat,
    SampleRate, StreamInstant,
    host::{Notify, latch::Latch},
    traits::StreamTrait,
};

pub(super) type InputDataCallback = dyn FnMut(&Data, &CallbackInfo) + Send + 'static;
pub(super) type OutputDataCallback = dyn FnMut(&mut Data, &CallbackInfo) + Send + 'static;
pub(super) type DuplexDataCallback =
    dyn FnMut(&Data, &mut Data, &DuplexCallbackInfo) + Send + 'static;
pub(super) type ErrorCallback = dyn FnMut(Error) + Send + 'static;

#[derive(Debug)]
pub struct Stream {
    /// The high-priority audio processing thread calling callbacks.
    /// Option used for moving out in destructor.
    thread: Option<JoinHandle<()>>,

    /// Single-direction or duplex.
    kind: StreamKind,

    /// Used to signal to stop processing.
    trigger: TriggerSender,

    /// Keeps the read end of the self-pipe alive for the lifetime of the Stream, so that
    /// `trigger.wakeup()` never writes to a closed pipe, even if the worker exited early.
    _rx: Arc<TriggerReceiver>,

    /// Latch that blocks the worker thread until `start()` is called for the first time.
    latch: Latch,
}

#[derive(Debug)]
enum StreamKind {
    Single(Arc<StreamInner>),
    Duplex(Arc<DuplexStreamInner>),
}

impl Stream {
    /// Parks the worker and gets exclusive access to the dsp fd.
    fn park_worker(&self) {
        self.latch.release();
        // Must be true before the trigger fires, so the worker sees it on the next loop iteration.
        match &self.kind {
            StreamKind::Single(inner) => {
                inner.control.parked.store(true, Ordering::Relaxed);
                self.trigger.wakeup();
                inner.control.park_worker();
            }
            StreamKind::Duplex(inner) => {
                inner.control.parked.store(true, Ordering::Relaxed);
                self.trigger.wakeup();
                inner.control.park_worker();
            }
        }
    }

    pub(super) fn new_input<D, E>(
        inner: Arc<StreamInner>,
        mut data_callback: D,
        mut error_callback: E,
        timeout: Option<Duration>,
    ) -> Stream
    where
        D: FnMut(&Data, &CallbackInfo) + Send + 'static,
        E: FnMut(Error) + Send + 'static,
    {
        let (tx, rx) = trigger();
        let rx_thread = rx.clone();
        let stream = inner.clone();

        // The latch is released by start(); the worker blocks here until then, keeping the
        // device untriggered with no DMA activity.
        let mut latch = Latch::new();
        let waiter = latch.waiter();

        let thread = thread::Builder::new()
            .name("cpal_oss_in".to_owned())
            .spawn(move || {
                waiter.wait();
                input_stream_worker(
                    rx_thread,
                    &stream,
                    &mut data_callback,
                    &mut error_callback,
                    timeout,
                );
            })
            .unwrap();
        latch.add_thread(thread.thread().clone());

        Self {
            thread: Some(thread),
            kind: StreamKind::Single(inner),
            trigger: tx,
            _rx: rx,
            latch,
        }
    }

    pub(super) fn new_output<D, E>(
        inner: Arc<StreamInner>,
        mut data_callback: D,
        mut error_callback: E,
        timeout: Option<Duration>,
    ) -> Stream
    where
        D: FnMut(&mut Data, &CallbackInfo) + Send + 'static,
        E: FnMut(Error) + Send + 'static,
    {
        let (tx, rx) = trigger();
        let rx_thread = rx.clone();
        let stream = inner.clone();

        // The latch is released by start(); the worker blocks here until then.
        let mut latch = Latch::new();
        let waiter = latch.waiter();

        let thread = thread::Builder::new()
            .name("cpal_oss_out".to_owned())
            .spawn(move || {
                waiter.wait();
                output_stream_worker(
                    rx_thread,
                    &stream,
                    &mut data_callback,
                    &mut error_callback,
                    timeout,
                );
            })
            .unwrap();
        latch.add_thread(thread.thread().clone());

        Self {
            thread: Some(thread),
            kind: StreamKind::Single(inner),
            trigger: tx,
            _rx: rx,
            latch,
        }
    }

    pub(super) fn new_duplex<D, E>(
        inner: Arc<DuplexStreamInner>,
        mut data_callback: D,
        mut error_callback: E,
        timeout: Option<Duration>,
    ) -> Stream
    where
        D: FnMut(&Data, &mut Data, &DuplexCallbackInfo) + Send + 'static,
        E: FnMut(Error) + Send + 'static,
    {
        let (tx, rx) = trigger();
        let rx_thread = rx.clone();
        let stream = inner.clone();

        // The latch is released by start(); the worker blocks here until then.
        let mut latch = Latch::new();
        let waiter = latch.waiter();

        let thread = thread::Builder::new()
            .name("cpal_oss_duplex".to_owned())
            .spawn(move || {
                waiter.wait();
                duplex_stream_worker(
                    rx_thread,
                    &stream,
                    &mut data_callback,
                    &mut error_callback,
                    timeout,
                );
            })
            .unwrap();
        latch.add_thread(thread.thread().clone());

        Self {
            thread: Some(thread),
            kind: StreamKind::Duplex(inner),
            trigger: tx,
            _rx: rx,
            latch,
        }
    }

    // Signals the worker to exit: marks it dropping, unblocks it from acknowledge_park()
    // if parked, and wakes it from poll. dropping must be set first so the
    // worker exits on re-entry rather than polling again.
    fn shutdown_worker(&self) {
        match &self.kind {
            StreamKind::Single(inner) => {
                inner.control.dropping.store(true, Ordering::Relaxed);
                inner.control.unpark_worker();
            }
            StreamKind::Duplex(inner) => {
                inner.control.dropping.store(true, Ordering::Relaxed);
                inner.control.unpark_worker();
            }
        }
        self.trigger.wakeup();
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // Unblock the worker in case the stream is dropped before start() was called.
        // Idempotent: no effect if the worker is already running.
        self.latch.release();
        self.shutdown_worker();
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Issues SNDCTL_DSP_SETTRIGGER once per halt/start cycle.
pub(super) fn arm_trigger(
    fd: RawFd,
    caps: i32,
    triggered: &AtomicBool,
    mask: i32,
) -> Result<(), Error> {
    if caps & ioctl::PCM_CAP_TRIGGER == 0 {
        return Ok(());
    }
    if triggered
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        ioctl::set_trigger(fd, mask).map_err(map_io_error)?;
    }
    Ok(())
}

/// Halts the device, clearing the trigger so the next start() re-arms it.
pub(super) fn halt_stream(fd: RawFd, triggered: &AtomicBool) -> Result<(), Error> {
    triggered.store(false, Ordering::Relaxed);
    ioctl::halt(fd).map_err(map_io_error)
}

impl StreamTrait for Stream {
    fn start(&self) -> Result<(), Error> {
        match &self.kind {
            StreamKind::Single(inner) => {
                inner.control.draining.store(false, Ordering::Relaxed);
                self.latch.release(); // idempotent: no-op after first call
                inner.control.unpark_worker(); // resume if stop() left it parked; no-op otherwise
                arm_trigger(
                    inner.fd.0,
                    inner.caps,
                    &inner.triggered,
                    match inner.direction {
                        DeviceDirection::Input => ioctl::PCM_ENABLE_INPUT,
                        _ => ioctl::PCM_ENABLE_OUTPUT,
                    },
                )?;
                Ok(())
            }
            StreamKind::Duplex(inner) => {
                inner.control.draining.store(false, Ordering::Relaxed);
                self.latch.release();
                inner.control.unpark_worker();
                begin_duplex_playback(inner)
            }
        }
    }

    fn pause(&self) -> Result<(), Error> {
        match &self.kind {
            StreamKind::Single(inner) => {
                inner.control.draining.store(true, Ordering::Relaxed);
                self.park_worker();
                let result = halt_stream(inner.fd.0, &inner.triggered);
                inner.control.unpark_worker();
                result
            }
            StreamKind::Duplex(inner) => {
                inner.control.draining.store(true, Ordering::Relaxed);
                self.park_worker();
                let result = halt_stream(inner.fd.0, &inner.triggered);
                inner.control.unpark_worker();
                result
            }
        }
    }

    fn stop(&self, timeout: Option<Duration>) -> Result<(), Error> {
        match &self.kind {
            StreamKind::Single(inner) => {
                inner.control.draining.store(true, Ordering::Relaxed);

                // Unlike pause(), stop() lets buffered playback drain first.
                let output = inner.direction != DeviceDirection::Input;
                self.park_worker();
                if output {
                    let window = crate::host::frames_to_duration(
                        inner.ring_frames as FrameCount,
                        inner.sample_rate,
                    );
                    crate::host::wait_for_drain(window, timeout);
                }
                let result = halt_stream(inner.fd.0, &inner.triggered);
                inner.control.unpark_worker();
                result
            }
            StreamKind::Duplex(inner) => {
                inner.control.draining.store(true, Ordering::Relaxed);
                self.park_worker();
                let window = crate::host::frames_to_duration(
                    inner.ring_frames as FrameCount,
                    inner.sample_rate,
                );
                crate::host::wait_for_drain(window, timeout);
                let result = halt_stream(inner.fd.0, &inner.triggered);
                inner.control.unpark_worker();
                result
            }
        }
    }

    fn now(&self) -> StreamInstant {
        let creation_instant = match &self.kind {
            StreamKind::Single(inner) => inner.creation_instant,
            StreamKind::Duplex(inner) => inner.creation_instant,
        };
        let d = Instant::now().duration_since(creation_instant);
        StreamInstant::new(d.as_secs(), d.subsec_nanos())
    }

    fn buffer_size(&self) -> Result<FrameCount, Error> {
        match &self.kind {
            StreamKind::Single(inner) => Ok(inner.period_size as FrameCount),
            StreamKind::Duplex(inner) => Ok(inner.period_size as FrameCount),
        }
    }
}

/// Negotiated stream geometry, shared by single-direction and duplex streams.
#[derive(Debug, Clone, Copy)]
pub(super) struct StreamParams {
    pub(super) sample_format: SampleFormat,
    pub(super) frame_size: usize,
    /// Frames per callback, from the driver-rounded fragment size.
    pub(super) period_size: usize,
    /// Total ring buffer size in frames.
    pub(super) ring_frames: usize,
}

/// Owned dsp fd. Halting before close matters: dsp_close() drains pending playback,
/// which can sleep several seconds in the kernel.
#[derive(Debug)]
pub(crate) struct DspFd(pub(super) libc::c_int);

impl Drop for DspFd {
    fn drop(&mut self) {
        let _ = ioctl::halt(self.0);
        unsafe {
            libc::close(self.0);
        }
    }
}

// Park/drop plumbing shared by StreamInner and DuplexStreamInner, giving StreamTrait
// exclusive worker access for pause/stop regardless of handle count.
#[derive(Debug, Default)]
pub(super) struct WorkerControl {
    // Set when the worker should stop polling, e.g. after a device disconnect.
    pub(super) dropping: AtomicBool,

    // Whether the user callback is currently suppressed.
    pub(super) draining: AtomicBool,

    // Set by stop() to request the worker pause for exclusive dsp access.
    pub(super) parked: AtomicBool,
    park: Notify,
}

impl WorkerControl {
    // Pauses the worker at its next loop iteration and waits for acknowledgment, or returns
    // early if it already exited. Caller holds exclusive dsp access until unpark_worker().
    fn park_worker(&self) {
        self.parked.store(true, Ordering::Relaxed);
        let (lock, cvar) = &self.park;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        // Exit if the worker acknowledged the park OR if the worker has exited (dropping=true).
        while !*guard && !self.dropping.load(Ordering::Relaxed) {
            guard = cvar.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
    }

    // Acknowledges a pending park, then sleeps until unpark_worker() is called.
    pub(super) fn acknowledge_park(&self) {
        let (lock, cvar) = &self.park;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        *guard = true;
        cvar.notify_one();
        while self.parked.load(Ordering::Relaxed) {
            guard = cvar.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
        *guard = false;
    }

    // Marks the stream dead and wakes any thread blocked in park_worker(), so an exit
    // other than a normal drop doesn't hang it.
    pub(super) fn signal_worker_exit(&self) {
        self.dropping.store(true, Ordering::Relaxed);
        let (lock, cvar) = &self.park;
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        cvar.notify_one();
    }

    // Releases the park: clears parked and wakes the worker from acknowledge_park().
    fn unpark_worker(&self) {
        let (lock, cvar) = &self.park;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        *guard = false;
        self.parked.store(false, Ordering::Relaxed);
        drop(guard);
        cvar.notify_one();
    }
}

#[derive(Debug)]
pub(super) struct StreamInner {
    // Controls the worker thread's lifecycle and pause/stop state.
    pub(super) control: WorkerControl,

    // Stream direction.
    pub(super) direction: DeviceDirection,

    // The dsp fd.
    pub(super) fd: DspFd,

    // Format of the samples.
    pub(super) sample_format: SampleFormat,

    // Sample rate of the stream.
    pub(super) sample_rate: SampleRate,

    // Cached values for performance in audio callback hot path.
    pub(super) frame_size: usize,
    pub(super) period_size: usize,
    pub(super) period_samples: usize,
    pub(super) ring_frames: usize,
    // Whether sample_format is I24/U24: the wire is 3-byte packed, the callback container
    // is 4 bytes per sample, and the worker converts between the two.
    pub(super) packed_24: bool,
    // Only used for Output direction.
    pub(super) equilibrium: Option<EquilibriumFill>,

    // Monotonic instant captured at stream creation. Timestamp origin for callback clocks.
    pub(super) creation_instant: Instant,

    // Xrun pending delivery to the data callback.
    pub(super) pending_xrun: AtomicBool,

    // Device capability mask from SNDCTL_DSP_GETCAPS.
    pub(super) caps: i32,

    // Whether SNDCTL_DSP_SETTRIGGER has been issued since the last halt.
    pub(super) triggered: AtomicBool,

    // Last observed xrun counters, for delta detection.
    pub(super) last_play_underruns: AtomicI32,
    pub(super) last_rec_overruns: AtomicI32,
}

// The fd is only touched from the worker thread and from pause/stop, which park it first.
unsafe impl Sync for StreamInner {}

impl StreamInner {
    pub(super) fn new(
        fd: DspFd,
        direction: DeviceDirection,
        sample_rate: SampleRate,
        params: StreamParams,
    ) -> Self {
        let StreamParams {
            sample_format,
            frame_size,
            period_size,
            ring_frames,
        } = params;
        let packed_24 = ioctl::is_packed_24(sample_format);
        let channels = frame_size / ioctl::wire_sample_size(sample_format);
        let period_samples = period_size * channels;
        let caps = ioctl::get_caps(fd.0).unwrap_or(0);
        let err_info = ioctl::get_error(fd.0);
        let (play_underruns, rec_overruns) = err_info
            .map(|info| (info.play_underruns, info.rec_overruns))
            .unwrap_or((0, 0));

        Self {
            control: WorkerControl::default(),
            direction,
            fd,
            sample_format,
            sample_rate,
            frame_size,
            period_size,
            period_samples,
            ring_frames,
            packed_24,
            equilibrium: (direction == DeviceDirection::Output).then(|| {
                EquilibriumFill::new(sample_format, period_samples * sample_format.sample_size())
            }),
            creation_instant: Instant::now(),
            pending_xrun: AtomicBool::new(false),
            caps,
            triggered: AtomicBool::new(false),
            last_play_underruns: AtomicI32::new(play_underruns),
            last_rec_overruns: AtomicI32::new(rec_overruns),
        }
    }
}

#[derive(Debug)]
pub(super) struct DuplexDirectionState {
    pub(super) sample_format: SampleFormat,
    pub(super) frame_size: usize,
    pub(super) period_samples: usize,
}

#[derive(Debug)]
pub(super) struct DuplexStreamInner {
    pub(super) control: WorkerControl,

    pub(super) fd: DspFd,

    pub(super) capture: DuplexDirectionState,
    pub(super) playback: DuplexDirectionState,

    pub(super) sample_rate: SampleRate,
    pub(super) frame_size: usize,
    pub(super) period_size: usize,
    pub(super) ring_frames: usize,
    pub(super) packed_24: bool,

    pub(super) playback_equilibrium: EquilibriumFill,

    pub(super) creation_instant: Instant,
    pub(super) pending_xrun: AtomicBool,

    pub(super) caps: i32,
    pub(super) triggered: AtomicBool,

    pub(super) last_play_underruns: AtomicI32,
    pub(super) last_rec_overruns: AtomicI32,
}

// The fd is only touched from the worker thread and from pause/stop, which park it first.
unsafe impl Sync for DuplexStreamInner {}

impl DuplexStreamInner {
    pub(super) fn new(fd: DspFd, sample_rate: SampleRate, params: StreamParams) -> Self {
        let StreamParams {
            sample_format,
            frame_size,
            period_size,
            ring_frames,
        } = params;
        let packed_24 = ioctl::is_packed_24(sample_format);
        let channels = frame_size / ioctl::wire_sample_size(sample_format);
        let period_samples = period_size * channels;
        let caps = ioctl::get_caps(fd.0).unwrap_or(0);
        let err_info = ioctl::get_error(fd.0);
        let (play_underruns, rec_overruns) = err_info
            .map(|info| (info.play_underruns, info.rec_overruns))
            .unwrap_or((0, 0));

        Self {
            control: WorkerControl::default(),
            fd,
            capture: DuplexDirectionState {
                sample_format,
                frame_size,
                period_samples,
            },
            playback: DuplexDirectionState {
                sample_format,
                frame_size,
                period_samples,
            },
            sample_rate,
            frame_size,
            period_size,
            ring_frames,
            packed_24,
            playback_equilibrium: EquilibriumFill::new(
                sample_format,
                period_samples * sample_format.sample_size(),
            ),
            creation_instant: Instant::now(),
            pending_xrun: AtomicBool::new(false),
            caps,
            triggered: AtomicBool::new(false),
            last_play_underruns: AtomicI32::new(play_underruns),
            last_rec_overruns: AtomicI32::new(rec_overruns),
        }
    }
}

/// Strategy for pre-filling an output buffer with the equilibrium value.
#[derive(Debug)]
pub(super) enum EquilibriumFill {
    /// Equilibrium is represented as a single repeating byte value.
    Byte(u8),
    /// A period-sized buffer pre-filled with the equilibrium value.
    Template(Box<[u8]>),
}

impl EquilibriumFill {
    /// Compute the equilibrium-fill strategy for the given sample format at stream creation.
    pub(super) fn new(sample_format: SampleFormat, period_bytes: usize) -> Self {
        if sample_format.is_int() || sample_format.is_float() {
            Self::Byte(0)
        } else if sample_format == SampleFormat::U8 {
            Self::Byte(crate::host::equilibrium::U8_EQUILIBRIUM_BYTE)
        } else if sample_format.is_dsd() {
            Self::Byte(crate::host::equilibrium::DSD_EQUILIBRIUM_BYTE)
        } else {
            // Multi-byte unsigned integer formats require a fill equal to the midpoint of their
            // range.
            debug_assert!(sample_format.is_uint());
            let mut template = vec![0u8; period_bytes].into_boxed_slice();
            crate::host::equilibrium::fill_equilibrium(&mut template, sample_format);
            Self::Template(template)
        }
    }

    #[inline]
    pub(super) fn fill(&self, buffer: &mut [u8]) {
        match self {
            Self::Byte(b) => buffer.fill(*b),
            Self::Template(t) => buffer.copy_from_slice(t),
        }
    }
}
