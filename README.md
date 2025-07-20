# spike-prime
`spike-prime` is a library wrapping the SPIKE Prime BLE GATT protocol. If you run into any issues, or have an issue with the crate, feel free to open a GitHub issue, and I'll respond as soon as I can.<br><br>

To use it, first create an `Adapter` using `btleplug`:
```rust
use spike_prime::prelude::*;

let manager = Manager::new().await?;
let adapter = manager.adapters().await?.drain(..).next().unwrap();
```
Then, receive a `SpikePrime` device:
```rust
let device = SpikePrime::scan_first(&adapter).await?;
```
You can also use `SpikePrime::scan()` to get a stream of devices:
```rust
let mut stream = SpikePrime::scan(&adapter).await?;
stream.next().await;
let device = stream.next().await.unwrap();
```
After you have the device, connect to it:
```rust
let connection = device.connect().await?;
```
Then, you can use the built-in functions to communicate with the device, or you can use `SpikeConnection::send_message` to send custom messages.