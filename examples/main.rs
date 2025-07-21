use std::{fs, path::PathBuf};

use clap::Parser;
use spike_prime::prelude::*;

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
    let device = SpikePrime::scan_first(&adapter).await?;
    println!("Device found!");
    let mut connection = device.connect().await?;
    println!("Connected!");
    let code = fs::read_to_string(args.file)?.replace("\n", "\r\n");
    println!("Uploading file...");
    //connection.enable_device_notifications().await?;
    println!("Device notifications enabled...");
    let _ = connection.clear_program_slot(0).await; // Ignore NACK
    println!("Program slot cleared!");
    connection
        .upload_program(0, "program.py".to_string(), code)
        .await?;
    println!("Uploaded!");
    connection.start_program(0).await?;
    println!("Program started");

    loop {
        if let Some(r) = connection.try_receive_message() {
            println!("{:?}", r?);
        }
        while let Some(c) = connection.try_console_notification() {
            print!("{}", c.console_message);
        }
    }
}
