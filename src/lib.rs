use std::pin::Pin;

use btleplug::api::{Central as _, CentralEvent, Peripheral as _, ScanFilter};
use btleplug::platform::{Adapter, Peripheral};
use futures::{Stream, StreamExt};
use uuid::Uuid;

pub mod error;

pub mod connection;

pub mod prelude {
    pub use crate::SpikePrime;
    pub use crate::connection::SpikeConnection;
    pub use crate::error::*;
    pub use btleplug::{api::Manager as _, platform::Manager};
}
use prelude::*;

/// Represents a SPIKE Prime device, before it has been connected to.
#[derive(Debug)]
pub struct SpikePrime(Peripheral);

const PRIME_SERVICE: Uuid = Uuid::from_bytes([
    0x00, 0x00, 0xFD, 0x02, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0x80, 0x5F, 0x9B, 0x34, 0xFB,
]);

impl SpikePrime {
    /// Scans bluetooth devices, looking for a SPIKE Prime, using an [`Adapter`].
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

    pub async fn scan_first(adapter: &Adapter) -> Result<Self> {
        Ok(Self::scan(adapter).await?.next().await.unwrap())
    }

    /// Connects to a [`SpikePrime`] by returning a [`SpikeConnection`].
    pub async fn connect(self) -> Result<SpikeConnection> {
        SpikeConnection::new(self.0).await
    }

    /// Finds the name of a [`SpikePrime`] without connecting to it.
    pub async fn name(&self) -> Option<String> {
        self.0.properties().await.ok()??.local_name
    }
}
