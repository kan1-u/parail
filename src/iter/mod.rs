pub mod filter;
pub mod filter_map;
pub mod map;

pub use filter::*;
pub use filter_map::*;
pub use map::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_map_filter() {
        let v = vec![1, 2, 3, 4, 5];
        let mut iter = v.into_iter().par_map(|i| i * 2).par_filter(|i| i % 4 == 0);
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(8));
        assert_eq!(iter.next(), None);
    }
}
