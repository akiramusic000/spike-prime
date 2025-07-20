//! Module for messages that can be sent to the SPIKE Prime, and received from the SPIKE Prime.

use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};
use from_variants::FromVariants;
use uuid::Uuid;

use crate::error::*;

/// Messages sent to the SPIKE Prime
#[derive(Debug, PartialEq, Eq, Hash, Clone, FromVariants)]
pub enum RxMessage<'a> {
    InfoRequest,
    StartFirmwareUploadRequest(StartFirmwareUploadRequest),
    StartFileUploadRequest(StartFileUploadRequest<'a>),
    TransferChunkRequest(TransferChunkRequest<'a>),
    BeginFirmwareUpdateRequest(BeginFirmwareUpdateRequest),
    SetHubNameRequest(SetHubNameRequest<'a>),
    GetHubNameRequest,
    DeviceUuidRequest,
    ProgramFlowRequest(ProgramFlowRequest),
    ClearSlotRequest(ClearSlotRequest),
    TunnelMessage(TunnelMessage<'a>),
    DeviceNotificationRequest(DeviceNotificationRequest),
}

impl<'a> RxMessage<'a> {
    pub fn serialize(self) -> Vec<u8> {
        match self {
            RxMessage::InfoRequest => vec![0x00],
            RxMessage::StartFirmwareUploadRequest(r) => r.serialize(),
            RxMessage::StartFileUploadRequest(r) => r.serialize(),
            RxMessage::TransferChunkRequest(r) => r.serialize(),
            RxMessage::BeginFirmwareUpdateRequest(r) => r.serialize(),
            RxMessage::SetHubNameRequest(r) => r.serialize(),
            RxMessage::GetHubNameRequest => vec![0x18],
            RxMessage::DeviceUuidRequest => vec![0x1a],
            RxMessage::ProgramFlowRequest(r) => r.serialize(),
            RxMessage::ClearSlotRequest(r) => r.serialize(),
            RxMessage::TunnelMessage(r) => r.serialize(),
            RxMessage::DeviceNotificationRequest(r) => r.serialize(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StartFirmwareUploadRequest {
    pub file_sha: [u8; 20],
    pub crc32: u32,
}

impl StartFirmwareUploadRequest {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x0a); // ID
        buf.extend_from_slice(&self.file_sha);
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StartFileUploadRequest<'a> {
    pub file_name: &'a str,
    pub program_slot: u8,
    pub crc32: u32,
}

impl<'a> StartFileUploadRequest<'a> {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x0c); // ID
        buf.extend_from_slice(&self.file_name.as_bytes()[..31.min(self.file_name.len())]);
        buf.push(0x00); // null-terminator
        buf.push(self.program_slot);
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct TransferChunkRequest<'a> {
    pub crc32: u32,
    pub payload: &'a [u8],
}

impl<'a> TransferChunkRequest<'a> {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x10); // ID
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len().min(u16::MAX as usize) as u16).to_le_bytes());
        buf.extend_from_slice(&self.payload[..(u16::MAX as usize).min(self.payload.len())]);
        buf
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BeginFirmwareUpdateRequest {
    pub file_sha: [u8; 20],
    pub crc32: u32,
}

impl BeginFirmwareUpdateRequest {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x14); // ID
        buf.extend_from_slice(&self.file_sha);
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SetHubNameRequest<'a> {
    pub name: &'a str,
}

impl<'a> SetHubNameRequest<'a> {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x16); // ID
        buf.extend_from_slice(&self.name.as_bytes()[..(29).min(self.name.len())]);
        buf.push(0x00);
        buf
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ProgramFlowRequest {
    pub program_action: ProgramAction,
    pub program_slot: u8,
}

impl ProgramFlowRequest {
    pub fn serialize(self) -> Vec<u8> {
        vec![0x1e, self.program_action as u8, self.program_slot]
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ClearSlotRequest {
    pub program_slot: u8,
}

impl ClearSlotRequest {
    pub fn serialize(self) -> Vec<u8> {
        vec![0x46, self.program_slot]
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct TunnelMessage<'a> {
    pub payload: &'a [u8],
}

impl<'a> TunnelMessage<'a> {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x32); // ID
        buf.extend_from_slice(&(self.payload.len().min(u16::MAX as usize) as u16).to_le_bytes());
        buf.extend_from_slice(&self.payload[..(u16::MAX as usize).min(self.payload.len())]);
        buf
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DeviceNotificationRequest {
    pub interval: u16,
}

impl DeviceNotificationRequest {
    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x28); // ID
        buf.extend_from_slice(&self.interval.to_le_bytes());
        buf
    }
}

/// Messages received from the SPIKE Prime
#[derive(Debug, PartialEq, Eq, Hash, Clone, FromVariants)]
pub enum TxMessage {
    InfoResponse(InfoResponse),
    StartFirmwareUploadResponse(StartFirmwareUploadResponse),
    StartFileUploadResponse(StartFileUploadResponse),
    TransferChunkResponse(TransferChunkResponse),
    BeginFirmwareUpdateResponse(BeginFirmwareUpdateResponse),
    SetHubNameResponse(SetHubNameResponse),
    GetHubNameResponse(GetHubNameResponse),
    DeviceUuidResponse(DeviceUuidResponse),
    ProgramFlowResponse(ProgramFlowResponse),
    ProgramFlowNotification(ProgramFlowNotification),
    ClearSlotResponse(ClearSlotResponse),
    ConsoleNotification(ConsoleNotification),
    DeviceNotificationResponse(DeviceNotificationResponse),
    DeviceNotification(DeviceNotification),
}

impl TxMessage {
    pub fn deserialize(data: Vec<u8>) -> Result<TxMessage> {
        let mut cursor = Cursor::new(data);
        match cursor.read_u8()? {
            0x01 => Ok(TxMessage::InfoResponse(InfoResponse::deserialize(cursor)?)),
            0x0b => Ok(TxMessage::StartFirmwareUploadResponse(
                StartFirmwareUploadResponse::deserialize(cursor)?,
            )),
            0x0d => Ok(TxMessage::StartFileUploadResponse(
                StartFileUploadResponse::deserialize(cursor)?,
            )),
            0x11 => Ok(TxMessage::TransferChunkResponse(
                TransferChunkResponse::deserialize(cursor)?,
            )),
            0x15 => Ok(TxMessage::BeginFirmwareUpdateResponse(
                BeginFirmwareUpdateResponse::deserialize(cursor)?,
            )),
            0x17 => Ok(TxMessage::SetHubNameResponse(
                SetHubNameResponse::deserialize(cursor)?,
            )),
            0x19 => Ok(TxMessage::GetHubNameResponse(
                GetHubNameResponse::deserialize(cursor)?,
            )),
            0x1b => Ok(TxMessage::DeviceUuidResponse(
                DeviceUuidResponse::deserialize(cursor)?,
            )),
            0x1f => Ok(TxMessage::ProgramFlowResponse(
                ProgramFlowResponse::deserialize(cursor)?,
            )),
            0x20 => Ok(TxMessage::ProgramFlowNotification(
                ProgramFlowNotification::deserialize(cursor)?,
            )),
            0x47 => Ok(TxMessage::ClearSlotResponse(
                ClearSlotResponse::deserialize(cursor)?,
            )),
            0x21 => Ok(TxMessage::ConsoleNotification(
                ConsoleNotification::deserialize(cursor)?,
            )),
            0x29 => Ok(TxMessage::DeviceNotificationResponse(
                DeviceNotificationResponse::deserialize(cursor)?,
            )),
            0x3c => Ok(TxMessage::DeviceNotification(
                DeviceNotification::deserialize(cursor)?,
            )),
            _ => Err(Error::UnknownMessage),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct InfoResponse {
    pub rpc_major: u8,
    pub rpc_minor: u8,
    pub rpc_build: u16,
    pub firmware_major: u8,
    pub firmware_minor: u8,
    pub firmware_build: u16,
    pub max_packet_size: u16,
    pub max_msg_size: u16,
    pub max_chunk_size: u16,
    pub product_group_device_type: u16,
}

impl InfoResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let rpc_major = cursor.read_u8()?;
        let rpc_minor = cursor.read_u8()?;
        let rpc_build = cursor.read_u16::<LittleEndian>()?;
        let firmware_major = cursor.read_u8()?;
        let firmware_minor = cursor.read_u8()?;
        let firmware_build = cursor.read_u16::<LittleEndian>()?;
        let max_packet_size = cursor.read_u16::<LittleEndian>()?;
        let max_msg_size = cursor.read_u16::<LittleEndian>()?;
        let max_chunk_size = cursor.read_u16::<LittleEndian>()?;
        let product_group_device_type = cursor.read_u16::<LittleEndian>()?;

        Ok(InfoResponse {
            rpc_major,
            rpc_minor,
            rpc_build,
            firmware_major,
            firmware_minor,
            firmware_build,
            max_packet_size,
            max_msg_size,
            max_chunk_size,
            product_group_device_type,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StartFirmwareUploadResponse {
    pub response_status: ResponseStatus,
    pub already_uploaded: u32,
}

impl StartFirmwareUploadResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        let already_uploaded = cursor.read_u32::<LittleEndian>()?;
        Ok(StartFirmwareUploadResponse {
            response_status,
            already_uploaded,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StartFileUploadResponse {
    pub response_status: ResponseStatus,
}

impl StartFileUploadResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(StartFileUploadResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct TransferChunkResponse {
    pub response_status: ResponseStatus,
}

impl TransferChunkResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(TransferChunkResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BeginFirmwareUpdateResponse {
    pub response_status: ResponseStatus,
}

impl BeginFirmwareUpdateResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(BeginFirmwareUpdateResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SetHubNameResponse {
    pub response_status: ResponseStatus,
}

impl SetHubNameResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(SetHubNameResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct GetHubNameResponse {
    pub name: String,
}

impl GetHubNameResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let name = read_str(&mut cursor)?;
        Ok(GetHubNameResponse { name })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DeviceUuidResponse {
    pub uuid: Uuid,
}

impl DeviceUuidResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let mut buf = [0; 16];
        cursor.read_exact(&mut buf)?;
        let uuid = Uuid::from_bytes(buf);
        Ok(DeviceUuidResponse { uuid })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ProgramFlowResponse {
    pub response_status: ResponseStatus,
}

impl ProgramFlowResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(ProgramFlowResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ProgramFlowNotification {
    pub program_action: ProgramAction,
}

impl ProgramFlowNotification {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let program_action = match cursor.read_u8()? {
            0 => ProgramAction::Start,
            1 => ProgramAction::Stop,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ProgramAction",
                    value: i,
                });
            }
        };
        Ok(ProgramFlowNotification { program_action })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ClearSlotResponse {
    pub response_status: ResponseStatus,
}

impl ClearSlotResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(ClearSlotResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ConsoleNotification {
    pub console_message: String,
}

impl ConsoleNotification {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let console_message = read_str(&mut cursor)?;
        Ok(ConsoleNotification { console_message })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DeviceNotificationResponse {
    pub response_status: ResponseStatus,
}

impl DeviceNotificationResponse {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let response_status = match cursor.read_u8()? {
            0 => ResponseStatus::Acknowledged,
            1 => ResponseStatus::NotAcknowledged,
            i => {
                return Err(Error::InvalidEnumValue {
                    enum_name: "ResponseStatus",
                    value: i,
                });
            }
        };
        Ok(DeviceNotificationResponse { response_status })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DeviceNotification {
    pub payload: Vec<DeviceMessage>,
}

impl DeviceNotification {
    pub fn deserialize(mut cursor: Cursor<Vec<u8>>) -> Result<Self> {
        let size = cursor.read_u16::<LittleEndian>()?;
        let start = cursor.position();
        let mut payload = Vec::new();
        while cursor.position() < start + size as u64 {
            payload.push(DeviceMessage::deserialize(&mut cursor)?);
        }
        Ok(DeviceNotification { payload })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DeviceMessage {
    /// The devices's battery in percentage
    DeviceBattery(u8),
    DeviceImuValues {
        up_face: HubFace,
        yaw_face: HubFace,
        yaw: i16,
        pitch: i16,
        roll: i16,
        accelerometer_x: i16,
        accelerometer_y: i16,
        accelerometer_z: i16,
        gyroscope_x: i16,
        gyroscope_y: i16,
        gyroscope_z: i16,
    },
    /// The brightness of the pixels on the device's matrix display
    Device5x5MatrixDisplay([u8; 25]),
    DeviceMotor {
        port: HubPort,
        motor_device_type: MotorDeviceType,
        absolute_position: i16,
        power: i16,
        speed: i8,
        position: i32,
    },
    DeviceForceSensor {
        port: HubPort,
        value: u8,
        pressure: bool,
    },
    DeviceColorSensor {
        port: HubPort,
        color: Option<Color>,
        red: u16,
        green: u16,
        blue: u16,
    },
    DeviceDistanceSensor {
        port: HubPort,
        distance: i16,
    },
    Device3x3ColorMatrix {
        port: HubPort,
        pixels: [u8; 9],
    },
}

impl DeviceMessage {
    pub fn deserialize(cursor: &mut Cursor<Vec<u8>>) -> Result<Self> {
        match cursor.read_u8()? {
            0x00 => Ok(Self::DeviceBattery(cursor.read_u8()?)),
            0x01 => Ok(Self::DeviceImuValues {
                up_face: cursor.read_u8()?.try_into()?,
                yaw_face: cursor.read_u8()?.try_into()?,
                yaw: cursor.read_i16::<LittleEndian>()?,
                pitch: cursor.read_i16::<LittleEndian>()?,
                roll: cursor.read_i16::<LittleEndian>()?,
                accelerometer_x: cursor.read_i16::<LittleEndian>()?,
                accelerometer_y: cursor.read_i16::<LittleEndian>()?,
                accelerometer_z: cursor.read_i16::<LittleEndian>()?,
                gyroscope_x: cursor.read_i16::<LittleEndian>()?,
                gyroscope_y: cursor.read_i16::<LittleEndian>()?,
                gyroscope_z: cursor.read_i16::<LittleEndian>()?,
            }),
            0x02 => Ok(Self::Device5x5MatrixDisplay({
                let mut buf = [0; 25];
                cursor.read_exact(&mut buf)?;
                buf
            })),
            0x0a => Ok(Self::DeviceMotor {
                port: cursor.read_u8()?.try_into()?,
                motor_device_type: cursor.read_u8()?.try_into()?,
                absolute_position: cursor.read_i16::<LittleEndian>()?,
                power: cursor.read_i16::<LittleEndian>()?,
                speed: cursor.read_i8()?,
                position: cursor.read_i32::<LittleEndian>()?,
            }),
            0x0b => Ok(Self::DeviceForceSensor {
                port: cursor.read_u8()?.try_into()?,
                value: cursor.read_u8()?,
                pressure: match cursor.read_u8()? {
                    0x01 => true,
                    0x00 => false,
                    i => {
                        return Err(Error::InvalidEnumValue {
                            enum_name: "bool",
                            value: i,
                        });
                    }
                },
            }),
            0x0c => Ok(Self::DeviceColorSensor {
                port: cursor.read_u8()?.try_into()?,
                color: cursor.read_u8()?.try_into().ok(),
                red: cursor.read_u16::<LittleEndian>()?,
                green: cursor.read_u16::<LittleEndian>()?,
                blue: cursor.read_u16::<LittleEndian>()?,
            }),
            0x0d => Ok(Self::DeviceDistanceSensor {
                port: cursor.read_u8()?.try_into()?,
                distance: cursor.read_i16::<LittleEndian>()?,
            }),
            0x0e => Ok(Self::Device3x3ColorMatrix {
                port: cursor.read_u8()?.try_into()?,
                pixels: {
                    let mut buf = [0; 9];
                    cursor.read_exact(&mut buf)?;
                    buf
                },
            }),
            _ => Err(Error::UnknownMessage),
        }
    }
}

fn read_str(cursor: &mut Cursor<Vec<u8>>) -> Result<String> {
    let mut str = Vec::new();
    loop {
        let byte = cursor.read_u8()?;
        if byte == 0 {
            break;
        }
        str.push(byte);
    }
    let str = String::from_utf8(str).expect("string should be valid UTF-8");
    Ok(str)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProgramAction {
    Start = 0x00,
    Stop = 0x01,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ResponseStatus {
    Acknowledged = 0x00,
    NotAcknowledged = 0x01,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum HubFace {
    Top = 0x00,
    Front = 0x01,
    Right = 0x02,
    Bottom = 0x03,
    Back = 0x04,
    Left = 0x05,
}

impl TryFrom<u8> for HubFace {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x00 => Ok(HubFace::Top),
            0x01 => Ok(HubFace::Front),
            0x02 => Ok(HubFace::Right),
            0x03 => Ok(HubFace::Bottom),
            0x04 => Ok(HubFace::Back),
            0x05 => Ok(HubFace::Left),
            _ => Err(Error::InvalidEnumValue {
                enum_name: "HubFace",
                value,
            }),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum HubPort {
    A = 0x00,
    B = 0x01,
    C = 0x02,
    D = 0x03,
    E = 0x04,
    F = 0x05,
}

impl TryFrom<u8> for HubPort {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x00 => Ok(HubPort::A),
            0x01 => Ok(HubPort::B),
            0x02 => Ok(HubPort::C),
            0x03 => Ok(HubPort::D),
            0x04 => Ok(HubPort::E),
            0x05 => Ok(HubPort::F),
            _ => Err(Error::InvalidEnumValue {
                enum_name: "HubPort",
                value,
            }),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum MotorMoveDirection {
    Clockwise = 0x00,
    CounterClockwise = 0x01,
    ShortestPath = 0x02,
    LongestPath = 0x03,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum MotorDeviceType {
    Medium = 0x30,
    Large = 0x31,
    Small = 0x41,
}

impl TryFrom<u8> for MotorDeviceType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x30 => Ok(MotorDeviceType::Medium),
            0x31 => Ok(MotorDeviceType::Large),
            0x41 => Ok(MotorDeviceType::Small),
            _ => Err(Error::InvalidEnumValue {
                enum_name: "MotorDeviceType",
                value,
            }),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Color {
    Black = 0x00,
    Magenta = 0x01,
    Purple = 0x02,
    Blue = 0x03,
    Azure = 0x04,
    Turquoise = 0x05,
    Green = 0x06,
    Yellow = 0x07,
    Orange = 0x08,
    Red = 0x09,
    White = 0x0a,
}

impl TryFrom<u8> for Color {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Black),
            0x01 => Ok(Self::Magenta),
            0x02 => Ok(Self::Purple),
            0x03 => Ok(Self::Blue),
            0x04 => Ok(Self::Azure),
            0x05 => Ok(Self::Turquoise),
            0x06 => Ok(Self::Green),
            0x07 => Ok(Self::Yellow),
            0x08 => Ok(Self::Orange),
            0x09 => Ok(Self::Red),
            0x0a => Ok(Self::White),
            _ => Err(()),
        }
    }
}
