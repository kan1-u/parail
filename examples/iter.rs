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
