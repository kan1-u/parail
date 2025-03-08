use super::*;

pub struct ParFilterMap<T> {
    iter: ParMap<Option<T>>,
}

impl<T> Iterator for ParFilterMap<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find_map(|item| item)
    }
}

pub trait ParallelFilterMap<T> {
    fn par_filter_map<F, R>(self, filter_map_op: F) -> ParFilterMap<R>
    where
        F: Fn(T) -> Option<R> + Send + Sync + 'static,
        R: Send + 'static;
}

impl<T, I> ParallelFilterMap<T> for I
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send + 'static,
{
    fn par_filter_map<F, R>(self, filter_map_op: F) -> ParFilterMap<R>
    where
        F: Fn(T) -> Option<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        let iter = self.par_map(filter_map_op);
        ParFilterMap { iter }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_filter_map() {
        let v = vec![1, 2, 3, 4, 5];
        let mut iter = v
            .into_iter()
            .par_filter_map(|i| if i % 2 == 0 { Some(i) } else { None });
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), None);
    }
}
