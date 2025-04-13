use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use futures::StreamExt;
use parail::prelude::*;

#[inline]
fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        _ => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn bench_fibonacci(c: &mut Criterion) {
    for fib_n in [10, 20, 30] {
        let mut group = c.benchmark_group(format!("fibonacci/{}", fib_n));

        for num_elements in [1, 10, 100, 1_000, 10_000] {
            let elements = (0..num_elements).collect::<Vec<u64>>();

            group.bench_with_input(
                BenchmarkId::new("sequential", num_elements),
                &elements,
                |b, elements| {
                    b.iter_batched(
                        || elements.clone(),
                        move |elements| {
                            for n in elements
                                .into_iter()
                                .map(move |n| fibonacci(black_box(fib_n)) + n)
                            {
                                black_box(n);
                            }
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("iter_parallel", num_elements),
                &elements,
                |b, elements| {
                    b.iter_batched(
                        || elements.clone(),
                        move |elements| {
                            let iter = elements
                                .into_iter()
                                .par_map(move |n| fibonacci(black_box(fib_n)) + n);
                            for n in iter {
                                black_box(n);
                            }
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("stream_parallel", num_elements),
                &elements,
                |b, elements| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || futures::stream::iter(elements.clone()),
                            move |elements| async move {
                                let mut stream =
                                    elements.par_map(move |n| fibonacci(black_box(fib_n)) + n);
                                while let Some(n) = stream.next().await {
                                    black_box(n);
                                }
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
        }
    }
}

criterion_group!(benches, bench_fibonacci);
criterion_main!(benches);
