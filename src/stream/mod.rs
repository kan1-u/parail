pub mod filter;
pub mod filter_map;
pub mod map;

pub use filter::*;
pub use filter_map::*;
pub use map::*;

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_par_map_filter_stream() {
        let mut iter = futures::stream::iter(0..100)
            .par_map(|i| i * 2)
            .par_filter(|i| i % 4 == 0);
        for i in (0..100).map(|i| i * 2).filter(|&i| i % 4 == 0) {
            assert_eq!(iter.next().await, Some(i));
        }
    }
}
