//! Raw OSS ioctl declarations, matching FreeBSD's `<sys/soundcard.h>`.
//!
//! Declared in-crate against `libc::ioctl` so the backend needs no bindings dependency.

use std::{io, os::fd::RawFd};

use crate::SampleFormat;

// FreeBSD `_IOC(inout, group, num, len)` encoding.
const IOC_VOID: u64 = 0x2000_0000;
const IOC_OUT: u64 = 0x4000_0000;
const IOC_IN: u64 = 0x8000_0000;
const IOCPARM_MASK: u64 = 0x1fff;

const fn ioc(inout: u64, group: u8, num: u8, len: usize) -> libc::c_ulong {
    inout | (((len as u64) & IOCPARM_MASK) << 16) | ((group as u64) << 8) | num as u64
}

const DSP_MAGIC: u8 = b'P';

const SNDCTL_DSP_HALT: libc::c_ulong = ioc(IOC_VOID, DSP_MAGIC, 0, 0);
const SNDCTL_DSP_SPEED: libc::c_ulong = ioc(IOC_IN | IOC_OUT, DSP_MAGIC, 2, 4);
const SNDCTL_DSP_SETFMT: libc::c_ulong = ioc(IOC_IN | IOC_OUT, DSP_MAGIC, 5, 4);
const SNDCTL_DSP_CHANNELS: libc::c_ulong = ioc(IOC_IN | IOC_OUT, DSP_MAGIC, 6, 4);
const SNDCTL_DSP_SETFRAGMENT: libc::c_ulong = ioc(IOC_IN | IOC_OUT, DSP_MAGIC, 10, 4);
const SNDCTL_DSP_GETFMTS: libc::c_ulong = ioc(IOC_OUT, DSP_MAGIC, 11, 4);
const SNDCTL_DSP_GETOSPACE: libc::c_ulong = ioc(IOC_OUT, DSP_MAGIC, 12, 16);
const SNDCTL_DSP_GETISPACE: libc::c_ulong = ioc(IOC_OUT, DSP_MAGIC, 13, 16);
const SNDCTL_DSP_GETCAPS: libc::c_ulong = ioc(IOC_OUT, DSP_MAGIC, 15, 4);
const SNDCTL_DSP_SETTRIGGER: libc::c_ulong = ioc(IOC_IN, DSP_MAGIC, 16, 4);
const SNDCTL_DSP_GETODELAY: libc::c_ulong = ioc(IOC_OUT, DSP_MAGIC, 23, 4);
const SNDCTL_DSP_GETERROR: libc::c_ulong = ioc(IOC_OUT, DSP_MAGIC, 25, 112);
const SNDCTL_DSP_COOKEDMODE: libc::c_ulong = ioc(IOC_IN, DSP_MAGIC, 30, 4);

pub(crate) const AFMT_U8: u32 = 0x0000_0008;
pub(crate) const AFMT_S8: u32 = 0x0000_0040;
#[cfg(target_endian = "little")]
pub(crate) const AFMT_S16_NE: u32 = 0x0000_0010;
#[cfg(target_endian = "big")]
pub(crate) const AFMT_S16_NE: u32 = 0x0000_0020;
#[cfg(target_endian = "little")]
pub(crate) const AFMT_S32_NE: u32 = 0x0000_1000;
#[cfg(target_endian = "big")]
pub(crate) const AFMT_S32_NE: u32 = 0x0000_2000;
#[cfg(target_endian = "little")]
pub(crate) const AFMT_S24_NE: u32 = 0x0001_0000;
#[cfg(target_endian = "big")]
pub(crate) const AFMT_S24_NE: u32 = 0x0002_0000;
#[cfg(target_endian = "little")]
pub(crate) const AFMT_F32_NE: u32 = 0x1000_0000;
#[cfg(target_endian = "big")]
pub(crate) const AFMT_F32_NE: u32 = 0x2000_0000;

pub(crate) const PCM_ENABLE_INPUT: i32 = 0x0000_0001;
pub(crate) const PCM_ENABLE_OUTPUT: i32 = 0x0000_0002;

pub(crate) const PCM_CAP_TRIGGER: i32 = 0x0000_1000;

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct AudioBufInfo {
    pub(crate) fragments: libc::c_int,
    pub(crate) fragstotal: libc::c_int,
    pub(crate) fragsize: libc::c_int,
    pub(crate) bytes: libc::c_int,
}

#[repr(C)]
#[derive(Debug, Default)]
pub(crate) struct AudioErrInfo {
    pub(crate) play_underruns: libc::c_int,
    pub(crate) rec_overruns: libc::c_int,
    pub(crate) play_ptradjust: libc::c_uint,
    pub(crate) rec_ptradjust: libc::c_uint,
    pub(crate) play_errorcount: libc::c_int,
    pub(crate) rec_errorcount: libc::c_int,
    pub(crate) play_lasterror: libc::c_int,
    pub(crate) rec_lasterror: libc::c_int,
    pub(crate) play_errorparm: libc::c_long,
    pub(crate) rec_errorparm: libc::c_long,
    pub(crate) filler: [libc::c_int; 16],
}

fn last_err() -> io::Error {
    io::Error::last_os_error()
}

pub(crate) fn halt(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is a valid OSS device descriptor and HALT takes no argument.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_HALT, 0) } < 0 {
        return Err(last_err());
    }
    Ok(())
}

pub(crate) fn set_cooked(fd: RawFd, enabled: bool) -> io::Result<()> {
    let value = libc::c_int::from(enabled);
    // SAFETY: `value` is a valid `int` pointer and is not retained by the kernel.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_COOKEDMODE, &value) } < 0 {
        return Err(last_err());
    }
    Ok(())
}

pub(crate) fn set_format(fd: RawFd, format: u32) -> io::Result<u32> {
    let mut value = format;
    // SAFETY: `value` points to a valid `u32` the driver reads and overwrites with the
    // negotiated format.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_SETFMT, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(value)
}

pub(crate) fn set_channels(fd: RawFd, channels: i32) -> io::Result<i32> {
    let mut value = channels;
    // SAFETY: `value` points to a valid `int` the driver reads and overwrites with the
    // negotiated channel count.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_CHANNELS, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(value)
}

pub(crate) fn set_speed(fd: RawFd, rate: i32) -> io::Result<i32> {
    let mut value = rate;
    // SAFETY: `value` points to a valid `int` the driver reads and overwrites with the
    // negotiated sample rate.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_SPEED, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(value)
}

pub(crate) fn set_fragment(fd: RawFd, fragments: i32, frag_size_exp: i32) -> io::Result<()> {
    let mut value = (fragments << 16) | frag_size_exp;
    // SAFETY: `value` points to a valid `int` encoding (fragments << 16) | exponent.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_SETFRAGMENT, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(())
}

pub(crate) fn get_fmts(fd: RawFd) -> io::Result<i32> {
    let mut value = 0;
    // SAFETY: `value` points to a valid `int` receiving the format mask.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_GETFMTS, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(value)
}

pub(crate) fn get_caps(fd: RawFd) -> io::Result<i32> {
    let mut value = 0;
    // SAFETY: `value` points to a valid `int` receiving the capability mask.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_GETCAPS, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(value)
}

pub(crate) fn get_ospace(fd: RawFd) -> io::Result<AudioBufInfo> {
    let mut info = AudioBufInfo::default();
    // SAFETY: `info` is a valid `audio_buf_info` receiving the playback buffer state.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_GETOSPACE, &mut info) } < 0 {
        return Err(last_err());
    }
    Ok(info)
}

pub(crate) fn get_ispace(fd: RawFd) -> io::Result<AudioBufInfo> {
    let mut info = AudioBufInfo::default();
    // SAFETY: `info` is a valid `audio_buf_info` receiving the capture buffer state.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_GETISPACE, &mut info) } < 0 {
        return Err(last_err());
    }
    Ok(info)
}

pub(crate) fn get_odelay(fd: RawFd) -> io::Result<i32> {
    let mut value = 0;
    // SAFETY: `value` points to a valid `int` receiving the playback delay in bytes.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_GETODELAY, &mut value) } < 0 {
        return Err(last_err());
    }
    Ok(value)
}

pub(crate) fn set_trigger(fd: RawFd, mask: i32) -> io::Result<()> {
    let value = mask;
    // SAFETY: `value` points to a valid `int` holding the trigger mask.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_SETTRIGGER, &value) } < 0 {
        return Err(last_err());
    }
    Ok(())
}

pub(crate) fn get_error(fd: RawFd) -> io::Result<AudioErrInfo> {
    let mut info = AudioErrInfo::default();
    // SAFETY: `info` is a valid `audio_errinfo` receiving the error counters.
    if unsafe { libc::ioctl(fd, SNDCTL_DSP_GETERROR, &mut info) } < 0 {
        return Err(last_err());
    }
    Ok(info)
}

/// Maps a cpal sample format to its native-endian OSS `AFMT_*` bit.
///
/// I24/U24 map to the packed `AFMT_S24_*`; the worker repacks between the 4-byte cpal
/// container and the 3-byte wire layout (see `worker::pack_24`/`worker::unpack_24`).
pub(crate) fn afmt_for_format(format: SampleFormat) -> Option<u32> {
    match format {
        SampleFormat::I8 => Some(AFMT_S8),
        SampleFormat::U8 => Some(AFMT_U8),
        SampleFormat::I16 => Some(AFMT_S16_NE),
        SampleFormat::I24 | SampleFormat::U24 => Some(AFMT_S24_NE),
        SampleFormat::I32 => Some(AFMT_S32_NE),
        SampleFormat::F32 => Some(AFMT_F32_NE),
        _ => None,
    }
}

/// Whether the format needs 3-byte-packing conversion on the wire.
pub(crate) fn is_packed_24(format: SampleFormat) -> bool {
    matches!(format, SampleFormat::I24 | SampleFormat::U24)
}

/// Bytes per sample on the wire: 3 for the packed 24-bit formats, else the container size.
pub(crate) fn wire_sample_size(format: SampleFormat) -> usize {
    if is_packed_24(format) {
        3
    } else {
        format.sample_size()
    }
}
