use std::io::{Write, stdin, stdout};

use btleplug::{api::Manager as _, platform::Manager};
use futures::StreamExt;
use spike_prime::{Result, SpikePrime};

#[tokio::main]
async fn main() -> Result<()> {
    let manager = Manager::new().await?;
    let adapter = manager.adapters().await?.drain(..).next().unwrap();
    println!("Scanning for SPIKE Prime hubs");
    let mut stream = SpikePrime::scan(&adapter).await?;
    let device = stream.next().await.unwrap();
    println!("Device found!");
    let mut connection = device.connect().await?;
    println!("Connected!");
    let name = connection.get_hub_name().await?;
    print!("The hub's current name is {name}. Would you like to change it? ");
    stdout().flush()?;
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    if buffer.trim().starts_with("y") {
        buffer.clear();
        print!("What would you like to change it to? ");
        stdout().flush()?;
        stdin().read_line(&mut buffer)?;
        buffer.remove(buffer.len() - 1);
        connection.set_hub_name(&buffer).await?;
    } else {
        println!("Aborting...")
    }

    Ok(())
}
