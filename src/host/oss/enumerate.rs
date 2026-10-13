use std::collections::HashSet;

use super::{DEFAULT_DEVICE, Device, ioctl, open_dsp};
use crate::{DeviceDirection, Error};

/// Iterator over available OSS dsp devices.
pub type Devices = std::vec::IntoIter<Device>;

pub(super) fn enumerate_devices() -> Result<Devices, Error> {
    let mut devices = Vec::new();
    let mut seen = HashSet::new();

    // The default device is always available when OSS is present.
    if std::path::Path::new(DEFAULT_DEVICE).exists() {
        seen.insert(DEFAULT_DEVICE.to_owned());
        let (input, output) = probe_directions(DEFAULT_DEVICE);
        devices.push(Device {
            path: DEFAULT_DEVICE.to_owned(),
            desc: Some("Default Audio Device".to_owned()),
            direction: direction_from(input, output, DeviceDirection::Unknown),
        });
    }

    // Prefer /dev/sndstat for human-readable names; fall back to scanning /dev/dsp* nodes.
    let entries = sndstat_devices().unwrap_or_else(dsp_nodes);
    for (index, desc, hint) in entries {
        let path = format!("/dev/dsp{index}");
        if !seen.insert(path.clone()) {
            continue;
        }
        let (input, output) = probe_directions(&path);
        if !input && !output {
            continue;
        }
        devices.push(Device {
            path,
            desc: Some(desc),
            direction: direction_from(input, output, hint),
        });
    }

    Ok(devices.into_iter())
}

fn direction_from(input: bool, output: bool, fallback: DeviceDirection) -> DeviceDirection {
    match (input, output) {
        (true, true) => DeviceDirection::Duplex,
        (true, false) => DeviceDirection::Input,
        (false, true) => DeviceDirection::Output,
        (false, false) => fallback,
    }
}

/// Probes which directions a dsp node can be opened in. Each open is verified with
/// SNDCTL_DSP_GETCAPS so dead or permission-denied nodes are skipped.
fn probe_directions(path: &str) -> (bool, bool) {
    let input =
        open_dsp(path, DeviceDirection::Input).is_ok_and(|fd| ioctl::get_caps(fd.0).is_ok());
    let output =
        open_dsp(path, DeviceDirection::Output).is_ok_and(|fd| ioctl::get_caps(fd.0).is_ok());
    (input, output)
}

/// Parses `/dev/sndstat` lines like `pcm5: <BEHRINGER X-USB> (play/rec) default` into
/// (index, description, direction hint) entries.
fn sndstat_devices() -> Option<Vec<(u32, String, DeviceDirection)>> {
    let text = std::fs::read_to_string("/dev/sndstat").ok()?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("pcm") else {
            continue;
        };
        let Some((index, rest)) = rest.split_once(':') else {
            continue;
        };
        let Ok(index) = index.trim().parse::<u32>() else {
            continue;
        };
        let hint = if rest.contains("(play/rec)") {
            DeviceDirection::Duplex
        } else if rest.contains("(rec)") {
            DeviceDirection::Input
        } else if rest.contains("(play)") {
            DeviceDirection::Output
        } else {
            DeviceDirection::Duplex
        };
        let desc = parse_description(rest);
        out.push((index, desc, hint));
    }
    (!out.is_empty()).then_some(out)
}

fn parse_description(rest: &str) -> String {
    if let Some((_, after)) = rest.split_once('<') {
        if let Some((desc, _)) = after.split_once('>') {
            return desc.trim().to_owned();
        }
    }
    rest.trim().to_owned()
}

/// Fallback: scans /dev for `dspN` nodes when /dev/sndstat is unavailable.
fn dsp_nodes() -> Vec<(u32, String, DeviceDirection)> {
    let Ok(dir) = std::fs::read_dir("/dev") else {
        return Vec::new();
    };
    let mut out: Vec<_> = dir
        .filter_map(|entry| {
            let name = entry.ok()?.file_name();
            let name = name.to_str()?;
            let index = name.strip_prefix("dsp")?.parse::<u32>().ok()?;
            Some((index, name.to_owned(), DeviceDirection::Duplex))
        })
        .collect();
    out.sort_by_key(|(index, _, _)| *index);
    out
}
