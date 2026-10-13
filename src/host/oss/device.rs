use std::{fmt, vec::IntoIter as VecIntoIter};

use super::{
    DEFAULT_PERIOD_FRAMES, DEFAULT_PERIODS, MAX_FRAGMENT_BYTES, Stream, ioctl, map_io_error,
    open_dsp, stream::StreamParams, unsupported_config,
};
use crate::{
    BufferSize, ChannelCount, DeviceDescription, DeviceDescriptionBuilder, DeviceDirection,
    DeviceId, DuplexCallbackInfo, DuplexStreamConfig, Error, SAMPLE_RATE_48K, SampleFormat,
    SampleRate, StreamConfig, SupportedBufferSize, SupportedStreamConfig,
    SupportedStreamConfigRange,
    iter::{SupportedInputConfigs, SupportedOutputConfigs},
    traits::DeviceTrait,
};

/// Candidate rates probed with `SNDCTL_DSP_SPEED` when building supported config lists.
const PROBE_RATES: [SampleRate; 9] = [
    8000, 16000, 22050, 44100, 48000, 88200, 96000, 176400, 192000,
];

/// Candidate channel counts probed with `SNDCTL_DSP_CHANNELS`.
const PROBE_CHANNELS: [ChannelCount; 9] = [1, 2, 4, 6, 8, 12, 16, 24, 32];

#[derive(Clone, Debug)]
pub struct Device {
    pub(super) path: String,
    pub(super) desc: Option<String>,
    pub(super) direction: DeviceDirection,
}

impl DeviceTrait for Device {
    type SupportedInputConfigs = SupportedInputConfigs;
    type SupportedOutputConfigs = SupportedOutputConfigs;
    type Stream = Stream;

    fn description(&self) -> Result<DeviceDescription, Error> {
        Self::description(self)
    }

    fn id(&self) -> Result<DeviceId, Error> {
        Self::id(self)
    }

    // Override trait defaults to avoid opening devices during enumeration.
    fn supports_input(&self) -> bool {
        matches!(
            self.direction,
            DeviceDirection::Input | DeviceDirection::Duplex
        )
    }

    fn supports_output(&self) -> bool {
        matches!(
            self.direction,
            DeviceDirection::Output | DeviceDirection::Duplex
        )
    }

    fn supports_duplex(&self) -> bool {
        self.direction == DeviceDirection::Duplex
    }

    fn supported_input_configs(&self) -> Result<Self::SupportedInputConfigs, Error> {
        Self::supported_configs(self, DeviceDirection::Input)
    }

    fn supported_output_configs(&self) -> Result<Self::SupportedOutputConfigs, Error> {
        Self::supported_configs(self, DeviceDirection::Output)
    }

    fn default_input_config(&self) -> Result<SupportedStreamConfig, Error> {
        Self::default_config(self, DeviceDirection::Input)
    }

    fn default_output_config(&self) -> Result<SupportedStreamConfig, Error> {
        Self::default_config(self, DeviceDirection::Output)
    }

    fn build_input_stream_raw<D, E>(
        &self,
        conf: StreamConfig,
        sample_format: SampleFormat,
        data_callback: D,
        error_callback: E,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self::Stream, Error>
    where
        D: FnMut(&crate::Data, &crate::CallbackInfo) + Send + 'static,
        E: FnMut(Error) + Send + 'static,
    {
        // Keep the device timestamp monotonic: the capture delay estimate varies between cycles.
        let data_callback = crate::host::monotonic_input_callback(data_callback);
        let inner = self.build_stream_inner(conf, sample_format, DeviceDirection::Input)?;
        Ok(Stream::new_input(
            std::sync::Arc::new(inner),
            data_callback,
            error_callback,
            timeout,
        ))
    }

    fn build_output_stream_raw<D, E>(
        &self,
        conf: StreamConfig,
        sample_format: SampleFormat,
        data_callback: D,
        error_callback: E,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self::Stream, Error>
    where
        D: FnMut(&mut crate::Data, &crate::CallbackInfo) + Send + 'static,
        E: FnMut(Error) + Send + 'static,
    {
        // Keep the device timestamp monotonic: the playback delay estimate varies between cycles.
        let data_callback = crate::host::monotonic_output_callback(data_callback);
        let inner = self.build_stream_inner(conf, sample_format, DeviceDirection::Output)?;
        Ok(Stream::new_output(
            std::sync::Arc::new(inner),
            data_callback,
            error_callback,
            timeout,
        ))
    }

    fn build_duplex_stream_raw<D, E>(
        &self,
        config: DuplexStreamConfig,
        input_sample_format: SampleFormat,
        output_sample_format: SampleFormat,
        data_callback: D,
        error_callback: E,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self::Stream, Error>
    where
        D: FnMut(&crate::Data, &mut crate::Data, &DuplexCallbackInfo) + Send + 'static,
        E: FnMut(Error) + Send + 'static,
    {
        let data_callback = crate::host::monotonic_duplex_callback(data_callback);
        let inner =
            self.build_duplex_stream_inner(config, input_sample_format, output_sample_format)?;
        Ok(Stream::new_duplex(
            std::sync::Arc::new(inner),
            data_callback,
            error_callback,
            timeout,
        ))
    }
}

impl Device {
    fn build_stream_inner(
        &self,
        conf: StreamConfig,
        sample_format: SampleFormat,
        direction: DeviceDirection,
    ) -> Result<super::stream::StreamInner, Error> {
        crate::validate_stream_config(&conf)?;

        let fd = open_dsp(&self.path, direction)?.into_dsp_fd();
        let params = configure_stream(fd.0, &conf, sample_format, direction)?;

        let stream_inner = super::stream::StreamInner::new(fd, direction, conf.sample_rate, params);

        Ok(stream_inner)
    }

    fn build_duplex_stream_inner(
        &self,
        config: DuplexStreamConfig,
        input_sample_format: SampleFormat,
        output_sample_format: SampleFormat,
    ) -> Result<super::stream::DuplexStreamInner, Error> {
        let stream_config = StreamConfig {
            channels: config.input_channels,
            sample_rate: config.sample_rate,
            buffer_size: config.buffer_size,
        };
        crate::validate_stream_config(&stream_config)?;

        // A single O_RDWR fd drives both directions, so OSS can only offer one format,
        // channel count, and rate for the pair.
        if input_sample_format != output_sample_format {
            return Err(unsupported_config(
                "OSS duplex requires the same sample format for both directions",
            ));
        }
        if config.input_channels != config.output_channels {
            return Err(unsupported_config(
                "OSS duplex requires the same channel count for both directions",
            ));
        }

        let fd = open_dsp(&self.path, DeviceDirection::Duplex)?.into_dsp_fd();
        let params = configure_stream(
            fd.0,
            &stream_config,
            input_sample_format,
            DeviceDirection::Duplex,
        )?;

        Ok(super::stream::DuplexStreamInner::new(
            fd,
            config.sample_rate,
            params,
        ))
    }

    fn description(&self) -> Result<DeviceDescription, Error> {
        let name = self
            .desc
            .as_ref()
            .map(|desc| desc.lines().next().unwrap_or(desc.as_str()).to_owned())
            .unwrap_or_else(|| self.path.clone());

        let mut builder = DeviceDescriptionBuilder::new(name)
            .driver(self.path.as_str())
            .direction(self.direction);

        if let Some(ref desc) = self.desc {
            builder = builder.extended(desc.lines().map(|l| l.trim()).filter(|l| !l.is_empty()));
        }

        Ok(builder.build())
    }

    fn id(&self) -> Result<DeviceId, Error> {
        Ok(DeviceId::new(crate::platform::HostId::Oss, &self.path))
    }

    fn supported_configs(
        &self,
        direction: DeviceDirection,
    ) -> Result<VecIntoIter<SupportedStreamConfigRange>, Error> {
        let fd = open_dsp(&self.path, direction)?;
        let fmts = ioctl::get_fmts(fd.0).map_err(map_io_error)?;

        let mut output = Vec::new();
        // Probe each format on a fresh fd: rate/channel trials leave the device in the last
        // negotiated state, and a failed SETFMT would otherwise poison the next candidate.
        // Trial failures skip the candidate; the open and GETFMTS errors above are the
        // authoritative "device unusable" signal.
        for &sample_format in &[
            SampleFormat::F32,
            SampleFormat::I32,
            SampleFormat::I24,
            SampleFormat::U24,
            SampleFormat::I16,
            SampleFormat::U8,
            SampleFormat::I8,
        ] {
            let Some(afmt) = ioctl::afmt_for_format(sample_format) else {
                continue;
            };
            if fmts & afmt as i32 == 0 {
                continue;
            }
            let Ok(fd) = open_dsp(&self.path, direction) else {
                continue;
            };
            let Ok(negotiated) = ioctl::set_format(fd.0, afmt) else {
                continue;
            };
            if negotiated != afmt {
                continue;
            }
            for &channels in &PROBE_CHANNELS {
                let Ok(actual) = ioctl::set_channels(fd.0, i32::from(channels)) else {
                    break;
                };
                if actual != i32::from(channels) {
                    continue;
                }
                for &rate in &PROBE_RATES {
                    if let Ok(actual) = ioctl::set_speed(fd.0, rate as i32) {
                        if actual == rate as i32 {
                            output.push(SupportedStreamConfigRange {
                                channels,
                                min_sample_rate: rate,
                                max_sample_rate: rate,
                                buffer_size: SupportedBufferSize::Unknown,
                                sample_format,
                            });
                        }
                    }
                }
            }
        }

        Ok(output.into_iter())
    }

    fn default_config(&self, direction: DeviceDirection) -> Result<SupportedStreamConfig, Error> {
        let formats: Vec<_> = self.supported_configs(direction)?.collect();

        // Prefer the conventional OSS default: stereo, 48 kHz, 16-bit.
        if let Some(range) = formats.iter().find(|range| {
            range.channels == 2
                && range.sample_format == SampleFormat::I16
                && range.contains_rate(SAMPLE_RATE_48K)
        }) {
            return Ok(range.with_sample_rate(SAMPLE_RATE_48K));
        }

        match formats
            .into_iter()
            .max_by(|a, b| a.cmp_default_heuristics(b))
        {
            Some(f) => Ok(f
                .try_with_standard_sample_rate()
                .unwrap_or_else(|| f.with_max_sample_rate())),
            None => Err(unsupported_config("no supported configuration")),
        }
    }
}

/// Negotiates format, channels, rate, and fragment layout on an open dsp fd.
///
/// Returns the actual period (in frames) and ring buffer sizes: the driver rounds the
/// fragment size to a power of two, so the callback size can differ from the request.
pub(super) fn configure_stream(
    fd: libc::c_int,
    conf: &StreamConfig,
    sample_format: SampleFormat,
    direction: DeviceDirection,
) -> Result<StreamParams, Error> {
    // Disable cooked mode so the driver neither resamples nor converts formats silently.
    let _ = ioctl::set_cooked(fd, false);

    let afmt = ioctl::afmt_for_format(sample_format).ok_or_else(|| {
        unsupported_config(format!(
            "OSS does not support sample format {sample_format}"
        ))
    })?;
    let negotiated = ioctl::set_format(fd, afmt).map_err(map_io_error)?;
    if negotiated != afmt {
        return Err(unsupported_config(format!(
            "device negotiated sample format {negotiated:#x} instead of requested {afmt:#x}"
        )));
    }

    let channels = i32::from(conf.channels);
    let negotiated = ioctl::set_channels(fd, channels).map_err(map_io_error)?;
    if negotiated != channels {
        return Err(unsupported_config(format!(
            "device supports {negotiated} channels, requested {channels}"
        )));
    }

    let rate = conf.sample_rate as i32;
    let negotiated = ioctl::set_speed(fd, rate).map_err(map_io_error)?;
    if negotiated != rate {
        return Err(unsupported_config(format!(
            "device runs at {negotiated} Hz, requested {rate} Hz"
        )));
    }

    let frame_size = ioctl::wire_sample_size(sample_format) * conf.channels as usize;
    let period_frames = match conf.buffer_size {
        BufferSize::Fixed(frames) => frames as usize,
        BufferSize::Default => DEFAULT_PERIOD_FRAMES,
    };
    let period_bytes = period_frames * frame_size;
    let frag_bytes = period_bytes
        .next_power_of_two()
        .clamp(frame_size, MAX_FRAGMENT_BYTES);
    let frag_exp = frag_bytes.trailing_zeros() as i32;
    ioctl::set_fragment(fd, DEFAULT_PERIODS as i32, frag_exp).map_err(map_io_error)?;

    let info = match direction {
        DeviceDirection::Input => ioctl::get_ispace(fd),
        _ => ioctl::get_ospace(fd),
    }
    .map_err(map_io_error)?;

    let fragsize = info.fragsize.max(1) as usize;
    let period_size = (fragsize / frame_size).max(1);
    let ring_frames = (fragsize * info.fragstotal.max(1) as usize / frame_size).max(period_size);

    Ok(StreamParams {
        sample_format,
        frame_size,
        period_size,
        ring_frames,
    })
}

impl PartialEq for Device {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for Device {}

impl fmt::Display for Device {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let desc = self.description().map_err(|_| fmt::Error)?;
        f.write_str(desc.name())
    }
}

impl std::hash::Hash for Device {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}
