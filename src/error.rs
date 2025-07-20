//! Module for errors within the `spike-prime` crate.

use std::{error, fmt::Display, io};

/// Errors produced by `spike-prime`
#[derive(Debug)]
pub enum Error {
    /// Errors from the `blteplug` crate
    BluetoothError(btleplug::Error),
    /// I/O errors
    Io(io::Error),
    /// Produced when a device is connected to that isn't a SPIKE Prime. This error is pretty rare.
    BadDevice,
    /// Produced when a message is received from the device that isn't known in the SPIKE Prime protocol. Also pretty rare.
    UnknownMessage,
    /// Produced when a message is received from the device, when a different message should have been sent.
    WrongMessage,
    /// Produced when a message is attempted to be sent that is larger than the max message size.
    OversizedMessage,
    /// Produced when a message is received that is supposed to contain an enumeration, but the value of the enumeration is not valid.
    InvalidEnumValue { enum_name: &'static str, value: u8 },
    /// Produced when a message is "Not Acknowledged" by the device.
    NotAcknowledged(&'static str, Option<usize>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

/// Result type using [`Error`] for convenience.
pub type Result<T, E = Error> = std::result::Result<T, E>;
