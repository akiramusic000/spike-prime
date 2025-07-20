use std::{io::stdout, time::Duration};

use crossterm::execute;
use futures::StreamExt;
use spike_prime::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let manager = Manager::new().await?;
    let adapter = manager.adapters().await?.drain(..).next().unwrap();
    println!("Scanning for SPIKE Prime hubs");
    let mut stream = SpikePrime::scan(&adapter).await?;
    let device = stream.next().await.unwrap();
    println!("Device found!");
    let mut connection = device.connect().await?;
    connection.enable_device_notifications().await?;
    println!("Message sent");
    loop {
        let response = connection.device_notification().await;
        if let Some(n) = response {
            execute! {
                stdout(),
                crossterm::terminal::Clear(crossterm::terminal::ClearType::All),
                crossterm::cursor::MoveTo(0, 0),
            }?;
            println!("{n:?}");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
