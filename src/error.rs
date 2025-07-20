use std::{error, fmt::Display, io};

#[derive(Debug)]
pub enum Error {
    AdapterNotFound,
    BluetoothError(btleplug::Error),
    Io(io::Error),
    BadDevice,
    UnknownMessage,
    WrongMessage,
    OversizedMessage,
    InvalidEnumValue { enum_name: &'static str, value: u32 },
    NotAcknowledged(&'static str, Option<usize>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::AdapterNotFound => write!(f, "bluetooth adapter not found"),
            Error::BluetoothError(e) => write!(f, "{e}"),
            Error::Io(e) => write!(f, "{e}"),
            Error::BadDevice => write!(f, "tried to connect to a device that isn't a SPIKE Prime"),
            Error::UnknownMessage => write!(f, "tried to deserialize an invalid packet"),
            Error::WrongMessage => write!(f, "device sent incorrect packet"),
            Error::OversizedMessage => {
                write!(f, "tried to send a message over the max message size")
            }
            Error::InvalidEnumValue { enum_name, value } => {
                write!(f, "invalid value {value} for enum {enum_name}")
            }
            Error::NotAcknowledged(str, bytes) => write!(
                f,
                "{str} message not acknowledged{}",
                if let Some(b) = bytes {
                    format!(" at byte position {b}")
                } else {
                    "".to_string()
                }
            ),
        }
    }
}

impl error::Error for Error {}

impl From<btleplug::Error> for Error {
    fn from(e: btleplug::Error) -> Self {
        Self::BluetoothError(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
