use btleplug::api::Manager as _;
use btleplug::platform::Manager;
use spike_prime::SpikePrime;
use spike_prime::error::*;

#[tokio::test]
async fn barebones() -> Result<()> {
    let manager = Manager::new().await?;
    let adapter = manager.adapters().await?.drain(..).next().unwrap();
    let devices = SpikePrime::scan(&adapter).await?;
    // We can't really do anything now, since the testing environment won't always have a spike prime. In fact, it will almost never have a spike prime.
    drop(devices);

    Ok(())
}
