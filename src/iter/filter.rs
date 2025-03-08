use super::*;

pub struct ParFilter<T> {
    iter: ParMap<Option<T>>,
}

impl<T> Iterator for ParFilter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find_map(|item| item)
    }
}

pub trait ParallelFilter<T> {
    fn par_filter<F>(self, filter_op: F) -> ParFilter<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static;
}

impl<T, I> ParallelFilter<T> for I
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send + 'static,
{
    fn par_filter<F>(self, filter_op: F) -> ParFilter<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let iter = self.par_map(move |item| filter_op(&item).then(|| item));
        ParFilter { iter }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_filter() {
        let v = vec![1, 2, 3, 4, 5];
        let mut iter = v.into_iter().par_filter(|i| i % 2 == 0);
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), None);
    }
}
