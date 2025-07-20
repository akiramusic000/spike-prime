use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::{connection::message::*, error::*};
use btleplug::{
    api::{Characteristic, Peripheral as _, WriteType},
    platform::Peripheral,
};
use futures::StreamExt;
use tokio::{
    sync::{
        Mutex,
        mpsc::{self, Receiver},
    },
    task::JoinHandle,
};
use uuid::Uuid;

const DEVICE_NOTIFICATION_INTERVAL: u16 = 10;

pub mod message;

pub struct SpikeConnection {
    connection: Peripheral,
    /// RX (from the hub's perspective)
    rx: Characteristic,
    rpc_version: (u8, u8, u16),
    firmware_version: (u8, u8, u16),
    max_packet_size: u16,
    max_message_size: u16,
    max_chunk_size: u16,
    device_notification: Arc<Mutex<Option<DeviceNotification>>>,
    msg_rx: Receiver<Result<TxMessage>>,
    _msg_handle: JoinHandle<()>,
    filter_device_notifications: Arc<AtomicBool>,
}

impl std::fmt::Debug for SpikeConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpikeConnection")
            .field("rpc_version", &self.rpc_version)
            .field("firmware_version", &self.firmware_version)
            .field("max_packet_size", &self.max_packet_size)
            .field("max_message_size", &self.max_message_size)
            .field("max_chunk_size", &self.max_chunk_size)
            .finish()
    }
}

impl SpikeConnection {
    pub(crate) async fn new(connection: Peripheral) -> Result<Self> {
        const RX_UUID: Uuid = Uuid::from_bytes([
            0x00, 0x00, 0xFD, 0x02, 0x00, 0x01, 0x10, 0x00, 0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B,
            0x34, 0xFB,
        ]);
        const TX_UUID: Uuid = Uuid::from_bytes([
            0x00, 0x00, 0xFD, 0x02, 0x00, 0x02, 0x10, 0x00, 0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B,
            0x34, 0xFB,
        ]);

        connection.connect().await?;
        connection.discover_services().await?;
        let mut rx = None;
        let mut tx = None;
        for i in connection.characteristics() {
            match i.uuid {
                RX_UUID => {
                    rx = Some(i);
                }
                TX_UUID => {
                    tx = Some(i);
                }
                _ => {}
            }
        }

        let tx = tx.ok_or(Error::BadDevice)?;
        let rx = rx.ok_or(Error::BadDevice)?;

        connection.subscribe(&tx).await?;

        let info_request_packet = Self::encode_message(RxMessage::InfoRequest.serialize());
        connection
            .write(&rx, &info_request_packet, WriteType::WithoutResponse)
            .await?;

        let mut notifications = connection.notifications().await?;
        let response = Self::decode_message(notifications.next().await.unwrap().value);
        let packet = if let TxMessage::InfoResponse(r) = TxMessage::deserialize(response)? {
            r
        } else {
            Err(Error::WrongMessage)?
        };

        let rpc_version = (packet.rpc_major, packet.rpc_minor, packet.rpc_build);
        let firmware_version = (
            packet.firmware_major,
            packet.firmware_minor,
            packet.firmware_build,
        );

        if packet.product_group_device_type != 0 {
            return Err(Error::BadDevice);
        }

        let (msg_tx, msg_rx) = mpsc::channel(32 * 4); // Buffer holds 4 messages
        let device_notification = Arc::new(Mutex::new(None));

        let filter_device_notifications = Arc::new(AtomicBool::new(false));
        let fdn2 = filter_device_notifications.clone();
        let device_notification2 = device_notification.clone();

        let handle = tokio::spawn(async move {
            let device_notification = device_notification2;
            let filter_device_notifications = fdn2;
            let mut notifications = notifications;
            let mut buffer = Vec::new();

            loop {
                let device_notifications = filter_device_notifications.load(Ordering::Relaxed);
                let mut x = notifications.next().await.unwrap();
                buffer.append(&mut x.value);
                if buffer.ends_with(&[0x02]) {
                    let decode_buffer = Self::decode_message(buffer);
                    let message = TxMessage::deserialize(decode_buffer);
                    buffer = Vec::new();

                    if device_notifications && let Ok(TxMessage::DeviceNotification(r)) = message {
                        *device_notification.lock().await = Some(r);
                    } else {
                        msg_tx.send(message).await.expect("BUG");
                    }
                }
            }
        });

        Ok(SpikeConnection {
            connection,
            rx,
            rpc_version,
            firmware_version,
            max_packet_size: packet.max_packet_size,
            max_message_size: packet.max_msg_size,
            max_chunk_size: packet.max_chunk_size,
            msg_rx,
            _msg_handle: handle,
            filter_device_notifications,
            device_notification,
        })
    }

    /// Returns RPC Version as (major, minor, build)
    pub fn rpc_version(&self) -> (u8, u8, u16) {
        self.rpc_version
    }

    /// Returns Firmware Version as (major, minor, build)
    pub fn firmware_version(&self) -> (u8, u8, u16) {
        self.firmware_version
    }

    pub fn max_packet_size(&self) -> u16 {
        self.max_packet_size
    }

    pub fn max_message_size(&self) -> u16 {
        self.max_message_size
    }

    pub fn max_chunk_size(&self) -> u16 {
        self.max_chunk_size
    }

    pub async fn device_notification(&self) -> Option<DeviceNotification> {
        self.device_notification.lock().await.clone()
    }

    pub async fn send_message<'a, R: Into<RxMessage<'a>>>(&self, message: R) -> Result<()> {
        let message = message.into().serialize();
        println!("{message:?}");
        println!("{}", String::from_utf8_lossy(&message));
        if message.len() > self.max_message_size as usize {
            return Err(Error::OversizedMessage);
        }
        let bytes = Self::encode_message(message);
        for i in bytes.chunks(self.max_packet_size.into()) {
            self.write_bytes(i).await?;
        }

        Ok(())
    }

    pub async fn get_hub_name(&mut self) -> Result<String> {
        self.send_message(RxMessage::GetHubNameRequest).await?;
        if let TxMessage::GetHubNameResponse(r) = self.receive_message().await? {
            Ok(r.name)
        } else {
            Err(Error::WrongMessage)
        }
    }

    pub async fn get_device_uuid(&mut self) -> Result<Uuid> {
        self.send_message(RxMessage::DeviceUuidRequest).await?;
        let uuid = if let TxMessage::DeviceUuidResponse(r) = self.receive_message().await? {
            r.uuid
        } else {
            return Err(Error::WrongMessage);
        };
        Ok(uuid)
    }

    pub async fn set_hub_name(&mut self, name: &str) -> Result<()> {
        self.send_message(SetHubNameRequest {
            name
        }).await?;

        let status = if let TxMessage::SetHubNameResponse(r) = self.receive_message().await? {
            r.response_status
        } else {
            return Err(Error::WrongMessage);
        };
        if status == ResponseStatus::NotAcknowledged {
            return Err(Error::NotAcknowledged("SetHubNameRequest", None));
        }
        Ok(())
    }

    pub async fn enable_device_notifications(&mut self) -> Result<()> {
        self.send_message(DeviceNotificationRequest {
            interval: DEVICE_NOTIFICATION_INTERVAL,
        })
        .await?;
        let status = if let TxMessage::DeviceNotificationResponse(r) = self.receive_message().await? {
            r.response_status
        } else {
            return Err(Error::WrongMessage);
        };
        if status == ResponseStatus::NotAcknowledged {
            return Err(Error::NotAcknowledged("DeviceNotificationRequest", None));
        }

        self.filter_device_notifications
            .store(true, Ordering::Relaxed);

        Ok(())
    }

    pub async fn disable_device_notifications(&mut self) -> Result<()> {
        self.send_message(DeviceNotificationRequest { interval: 0 })
            .await?;

        self.filter_device_notifications
            .store(false, Ordering::Relaxed);
        *self.device_notification.lock().await = None;

        Ok(())
    }

    async fn write_bytes(&self, bytes: &[u8]) -> Result<()> {
        self.connection
            .write(&self.rx, bytes, WriteType::WithoutResponse)
            .await?;
        Ok(())
    }

    pub async fn receive_message(&mut self) -> Result<TxMessage> {
        self.msg_rx.recv().await.unwrap()
    }

    pub async fn send_chunks(&mut self, data: Vec<u8>) -> Result<()> {
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
        let mut digest = crc.digest();
        for i in (0..data.len()).step_by(self.max_chunk_size as usize) {
            let slice = &data[i..(i + self.max_chunk_size as usize).min(data.len())];
            digest.update(slice);
            for _ in 0..((4 - (slice.len() % 4)) % 4) {
                digest.update(&[0]);
            }

            let crc32 = digest.finalize();
            digest = crc.digest_with_initial(crc32);

            self.send_message(TransferChunkRequest {
                crc32,
                payload: slice,
            })
            .await?;
            let status = if let TxMessage::TransferChunkResponse(r) = self.receive_message().await?
            {
                r.response_status
            } else {
                return Err(Error::WrongMessage);
            };
            if status == ResponseStatus::NotAcknowledged {
                return Err(Error::NotAcknowledged("TransferChunkRequest", Some(i)));
            }
        }

        Ok(())
    }

    pub async fn start_program(&mut self, slot: u8) -> Result<()> {
        self.send_message(ProgramFlowRequest {
            program_action: ProgramAction::Start,
            program_slot: slot,
        })
        .await?;

        let status = if let TxMessage::ProgramFlowResponse(r) = self.receive_message().await? {
            r.response_status
        } else {
            return Err(Error::WrongMessage);
        };
        if status == ResponseStatus::NotAcknowledged {
            return Err(Error::NotAcknowledged("ProgramFlowRequest", None));
        }
        Ok(())
    }

    pub async fn upload_file(&mut self, slot: u8, name: String, code: String) -> Result<()> {
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
        let mut crc32 = crc.digest();

        crc32.update(code.as_bytes());

        for _ in 0..((4 - (code.len() % 4)) % 4) {
            crc32.update(&[0]);
        }
        let crc32 = crc32.finalize();
        println!("crc32: {crc32}");
        //let crc32 = 0;
        let message = StartFileUploadRequest {
            file_name: &name,
            program_slot: slot,
            crc32,
        };
        self.send_message(message).await?;
        let response = if let TxMessage::StartFileUploadResponse(r) = self.receive_message().await?
        {
            r.response_status
        } else {
            return Err(Error::WrongMessage);
        };
        if response == ResponseStatus::NotAcknowledged {
            return Err(Error::NotAcknowledged("StartFileUploadRequest", None));
        }
        self.send_chunks(code.into_bytes()).await?;

        Ok(())
    }

    pub async fn clear_program_slot(&mut self, slot: u8) -> Result<()> {
        self.send_message(ClearSlotRequest {
            program_slot: slot,
        }).await?;
        self.receive_message().await?;

        let status = if let TxMessage::ClearSlotResponse(r) = self.receive_message().await? {
            r.response_status
        } else {
            return Err(Error::WrongMessage);
        };
        if status == ResponseStatus::NotAcknowledged {
            return Err(Error::NotAcknowledged("ClearSlotResponse", None));
        }
        Ok(())
    }

    fn encode_message(data: Vec<u8>) -> Vec<u8> {
        const NO_DELIMITER: u8 = 0xff;
        const DELIMITER: u8 = 0x02;
        const MAX_BLOCK_SIZE: u8 = 84;
        const COBS_CODE_OFFSET: u8 = 0x02;

        let mut buf = vec![NO_DELIMITER];
        let mut code_index = 0;
        let mut block = 1;

        for byte in data {
            if byte > DELIMITER {
                buf.push(byte);
                block += 1;
            }

            if byte <= DELIMITER || block > MAX_BLOCK_SIZE {
                if byte <= DELIMITER {
                    let delimiter_base = byte * MAX_BLOCK_SIZE;
                    let block_offset = block + COBS_CODE_OFFSET;
                    buf[code_index] = delimiter_base + block_offset;
                }

                code_index = buf.len();
                buf.push(NO_DELIMITER);
                block = 1;
            }
        }

        buf[code_index] = block + COBS_CODE_OFFSET;
        buf.iter_mut().for_each(|x| *x ^= 0x03);
        buf.push(0x02);

        buf
    }

    fn decode_message(mut data: Vec<u8>) -> Vec<u8> {
        let mut start = 0;
        if data[0] == 0x01 {
            start += 1;
        }

        let neg_one = data.len() - 1;
        data[start..neg_one].iter_mut().for_each(|x| *x ^= 0x03);

        let mut buf = Vec::new();

        let (mut value, mut block) = Self::unescape(data[0]);
        for byte in &data[1..] {
            block -= 1;
            if block > 0 {
                buf.push(*byte);
                continue;
            }

            if let Some(val) = value {
                buf.push(val)
            }

            (value, block) = Self::unescape(*byte);
        }

        if buf.pop() != Some(0) {
            // Remove last 0
            panic!("removed something bad: {buf:?}");
        }
        buf
    }

    fn unescape(code: u8) -> (Option<u8>, u8) {
        const MAX_BLOCK_SIZE: u8 = 84;
        const COBS_CODE_OFFSET: u8 = 0x02;

        if code == 0xff {
            return (None, MAX_BLOCK_SIZE + 1);
        }

        let mut value = (code - COBS_CODE_OFFSET) / MAX_BLOCK_SIZE;
        let mut block = (code - COBS_CODE_OFFSET) % MAX_BLOCK_SIZE;

        if block == 0 {
            block = MAX_BLOCK_SIZE;
            value = value.wrapping_sub(1);
        }

        (Some(value), block)
    }
}
