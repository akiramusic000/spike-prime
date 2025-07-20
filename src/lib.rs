use std::pin::Pin;

use btleplug::api::{Central as _, CentralEvent, ScanFilter};
use btleplug::platform::{Adapter, Peripheral};
use futures::{Stream, StreamExt};
use uuid::Uuid;

pub mod error;
pub use error::*;

pub mod connection;
use crate::connection::SpikeConnection;

#[derive(Debug)]
pub struct SpikePrime(Peripheral);

const PRIME_SERVICE: Uuid = Uuid::from_bytes([
    0x00, 0x00, 0xFD, 0x02, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B, 0x34, 0xFB,
]);

impl SpikePrime {
    pub async fn scan<'a>(
        adapter: &'a Adapter,
    ) -> Result<Pin<Box<dyn Stream<Item = SpikePrime> + Send + 'a>>> {
        adapter
            .start_scan(ScanFilter {
                services: vec![PRIME_SERVICE],
            })
            .await?;

        let events = adapter
            .events()
            .await?
            .filter_map(async |event| match event {
                CentralEvent::DeviceDiscovered(id) => {
                    adapter.peripheral(&id).await.map(SpikePrime).ok()
                }
                _ => None,
            });

        Ok(Box::pin(events))
    }

    pub async fn connect(self) -> Result<SpikeConnection> {
        SpikeConnection::new(self.0).await
    }
}
