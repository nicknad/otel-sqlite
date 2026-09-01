use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Severity {
    #[default]
    Unspecified = 0,
    Trace = 1,
    Trace2 = 2,
    Trace3 = 3,
    Trace4 = 4,
    Debug = 5,
    Debug2 = 6,
    Debug3 = 7,
    Debug4 = 8,
    Info = 9,
    Info2 = 10,
    Info3 = 11,
    Info4 = 12,
    Warn = 13,
    Warn2 = 14,
    Warn3 = 15,
    Warn4 = 16,
    Error = 17,
    Error2 = 18,
    Error3 = 19,
    Error4 = 20,
    Fatal = 21,
    Fatal2 = 22,
    Fatal3 = 23,
    Fatal4 = 24,
}

impl Severity {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Trace),
            2 => Some(Self::Trace2),
            3 => Some(Self::Trace3),
            4 => Some(Self::Trace4),
            5 => Some(Self::Debug),
            6 => Some(Self::Debug2),
            7 => Some(Self::Debug3),
            8 => Some(Self::Debug4),
            9 => Some(Self::Info),
            10 => Some(Self::Info2),
            11 => Some(Self::Info3),
            12 => Some(Self::Info4),
            13 => Some(Self::Warn),
            14 => Some(Self::Warn2),
            15 => Some(Self::Warn3),
            16 => Some(Self::Warn4),
            17 => Some(Self::Error),
            18 => Some(Self::Error2),
            19 => Some(Self::Error3),
            20 => Some(Self::Error4),
            21 => Some(Self::Fatal),
            22 => Some(Self::Fatal2),
            23 => Some(Self::Fatal3),
            24 => Some(Self::Fatal4),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            _ => "UNSPECIFIED",
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
