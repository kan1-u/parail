use super::*;

pub struct ParFilter<T> {
    iter: ParMap<Option<T>>,
}

impl<T> ParFilter<T>
where
    T: Send + 'static,
{
    #[inline]
    pub(crate) fn new<I, F>(iter: I, filter_op: F) -> Self
    where
        I: Iterator<Item = T> + Send + 'static,
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let iter = ParMap::new(iter, move |item| filter_op(&item).then(|| item));
        ParFilter { iter }
    }
}

impl<T> Iterator for ParFilter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find_map(|item| item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.iter.size_hint().1)
    }
}

pub trait ParallelFilter {
    type Item;

    fn par_filter<F>(self, filter_op: F) -> ParFilter<Self::Item>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + 'static;
}

impl<I> ParallelFilter for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
    type Item = I::Item;

    fn par_filter<F>(self, filter_op: F) -> ParFilter<Self::Item>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + 'static,
    {
        ParFilter::new(self, filter_op)
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
