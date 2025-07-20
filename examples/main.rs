use std::{fs, path::PathBuf};

use btleplug::{api::Manager as _, platform::Manager};
use clap::Parser;
use futures::StreamExt;
use spike_prime::{Result, SpikePrime, connection::message::*};

#[derive(Parser)]
struct Args {
    file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let manager = Manager::new().await?;
    let adapter = manager.adapters().await?.drain(..).next().unwrap();
    println!("Scanning for SPIKE Prime hubs");
    let mut stream = SpikePrime::scan(&adapter).await?;
    let device = stream.next().await.unwrap();
    println!("Device found!");
    let mut connection = device.connect().await?;
    println!("Connected!");
    let code = fs::read_to_string(args.file)?.replace("\n", "\r\n");
    println!("Uploading file...");
    connection.enable_device_notifications().await?;
    connection.clear_program_slot(0).await?;
    connection
        .upload_file(0, "program.py".to_string(), code)
        .await?;
    connection.start_program(0).await?;

    loop {
        let response = connection.receive_message().await?;
        if let TxMessage::ConsoleNotification(r) = response {
            print!("{}", r.console_message);
        } else {
            println!("{response:?}");
        }
    }
}
