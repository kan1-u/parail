use futures::StreamExt;
use parail::prelude::*;

#[tokio::main]
async fn main() {
    let stream = futures::stream::iter(0..10);
    let result = stream.par_map_async(do_something).collect::<Vec<_>>().await;
    println!("result: {:?}", result);
}

async fn do_something(n: u64) -> u64 {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    println!("do something: {}", n);
    n + 1
}
