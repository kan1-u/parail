# Parail

Parail is easily switch between sequential and parallel.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
parail = { version = "0.1", features = ["iter", "stream"] }
```

## Examples

### Parallel for `Iterator`

Use `rayon` as a parallel processing backend.

```rust
use parail::prelude::*;

fn main() {
    let values = (0..10).collect::<Vec<u64>>();
    let result = values.into_iter().par_map(do_something).collect::<Vec<_>>();
    println!("result: {:?}", result);
}

fn do_something(n: u64) -> u64 {
    std::thread::sleep(std::time::Duration::from_secs(1));
    println!("do something: {}", n);
    n + 1
}
```

Output example:

```text
do something: 4
do something: 6
do something: 7
do something: 1
do something: 0
do something: 5
do something: 3
do something: 2
do something: 8
do something: 9
result: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

### Parallel for `futures::Stream`

Use `tokio` as a parallel processing backend.

```rust
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
```

Output example:

```text
do something: 6
do something: 8
do something: 9
do something: 0
do something: 1
do something: 2
do something: 7
do something: 3
do something: 5
do something: 4
result: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

## Implemented functions

More features will be added as needed.

- for `Iterator`
  - [x] par_map
  - [x] par_filter
  - [x] par_filter_map
- for `futures::Stream`
  - [x] par_map
  - [x] par_map_async
  - [x] par_filter
  - [x] par_filter_async
  - [x] par_filter_map
  - [x] par_filter_map_async
