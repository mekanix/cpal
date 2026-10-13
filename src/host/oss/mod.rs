//! OSS (Open Sound System) backend implementation.
//!
//! Default backend on FreeBSD.

use std::{io, mem, path::Path, time::Duration};

pub use self::device::Device;
pub use self::enumerate::Devices;
pub use self::stream::Stream;
use crate::{DeviceDirection, Error, ErrorKind, traits::HostTrait};

mod device;
mod duplex;
mod enumerate;
mod ioctl;
mod stream;
mod trigger;
mod worker;

/// The default FreeBSD host type.
#[derive(Debug, Clone)]
pub struct Host;

impl Host {
    pub fn new() -> Result<Self, Error> {
        Ok(Self)
    }
}

impl HostTrait for Host {
    type Devices = Devices;
    type Device = Device;

    fn is_available() -> bool {
        // OSS is usable whenever the default dsp node exists.
        Path::new(DEFAULT_DEVICE).exists()
    }

    fn devices(&self) -> Result<Self::Devices, Error> {
        enumerate::enumerate_devices()
    }

    fn default_input_device(&self) -> Option<Device> {
        Some(default_device())
    }

    fn default_output_device(&self) -> Option<Device> {
        Some(default_device())
    }
}

fn default_device() -> Device {
    Device {
        path: DEFAULT_DEVICE.to_owned(),
        desc: Some("Default Audio Device".to_owned()),
        direction: DeviceDirection::Unknown,
    }
}

pub(crate) const DEFAULT_DEVICE: &str = "/dev/dsp";
pub(crate) const DEFAULT_PERIOD_FRAMES: usize = 1024;
pub(crate) const DEFAULT_PERIODS: usize = 2;
pub(crate) const MAX_FRAGMENT_BYTES: usize = 1 << 16;

const POLL_INFINITE: i32 = -1; // "block until an event arrives"
pub(crate) const TRIGGER_PAYLOAD_SIZE: libc::ssize_t = mem::size_of::<u64>() as libc::ssize_t;

// Round up: a nonzero sub-millisecond timeout must not floor to 0 (a non-blocking poll),
// but an explicit Duration::ZERO stays 0 so a non-blocking poll can still be requested.
fn poll_timeout_millis(timeout: Option<Duration>) -> i32 {
    match timeout {
        Some(d) => d.as_nanos().div_ceil(1_000_000).min(i32::MAX as u128) as i32,
        None => POLL_INFINITE,
    }
}

/// Open flags for the given stream direction.
pub(super) fn open_flags(direction: DeviceDirection) -> libc::c_int {
    match direction {
        DeviceDirection::Input => libc::O_RDONLY,
        DeviceDirection::Output => libc::O_WRONLY,
        _ => libc::O_RDWR,
    }
}

/// Opens a dsp node non-blocking with close-on-exec.
pub(super) fn open_dsp(path: &str, direction: DeviceDirection) -> Result<FdGuard, Error> {
    let c_path = std::ffi::CString::new(path.as_bytes()).map_err(|_| {
        Error::with_message(
            ErrorKind::InvalidInput,
            format!("invalid device path {path:?}"),
        )
    })?;
    // SAFETY: `c_path` is a valid NUL-terminated path.
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            open_flags(direction) | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd == -1 {
        return Err(map_io_error(io::Error::last_os_error()));
    }
    Ok(FdGuard(fd))
}

/// Closes a raw file descriptor.
#[derive(Debug)]
pub(super) struct FdGuard(pub(super) libc::c_int);

impl FdGuard {
    /// Leaks the guard, returning the raw fd so it can be wrapped in a [`stream::DspFd`]
    /// which owns the different close semantics (halt before close).
    pub(super) fn into_dsp_fd(self) -> stream::DspFd {
        let this = mem::ManuallyDrop::new(self);
        stream::DspFd(this.0)
    }
}

impl Drop for FdGuard {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

/// Maps a raw OS error from an OSS call to a [`crate::Error`].
pub(super) fn map_io_error(err: io::Error) -> Error {
    match err.raw_os_error() {
        Some(libc::ENODEV) | Some(libc::ENOENT) => ErrorKind::DeviceNotAvailable.into(),
        Some(libc::EPERM) | Some(libc::EACCES) => ErrorKind::PermissionDenied.into(),
        Some(libc::EBUSY) => ErrorKind::DeviceBusy.into(),
        Some(libc::EINVAL) => ErrorKind::UnsupportedConfig.into(),
        Some(libc::ENOSYS) | Some(libc::ENOTSUP) => ErrorKind::UnsupportedOperation.into(),
        _ => Error::with_message(ErrorKind::BackendError, format!("OSS error: {err}")),
    }
}

/// Unsupported configuration error for a failed negotiation step.
pub(super) fn unsupported_config(message: impl Into<std::borrow::Cow<'static, str>>) -> Error {
    Error::with_message(ErrorKind::UnsupportedConfig, message)
}
