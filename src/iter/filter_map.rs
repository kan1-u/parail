use super::*;

pub struct ParFilterMap<T> {
    iter: ParMap<Option<T>>,
}

impl<T> ParFilterMap<T>
where
    T: Send + 'static,
{
    #[inline]
    pub(crate) fn new<I, F>(iter: I, filter_map_op: F) -> Self
    where
        I: Iterator + Send + 'static,
        I::Item: Send,
        F: Fn(I::Item) -> Option<T> + Send + Sync + 'static,
    {
        let iter = ParMap::new(iter, filter_map_op);
        ParFilterMap { iter }
    }
}

impl<T> Iterator for ParFilterMap<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find_map(|item| item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.iter.size_hint().1)
    }
}

pub trait ParallelFilterMap {
    type Item;

    fn par_filter_map<T, F>(self, filter_map_op: F) -> ParFilterMap<T>
    where
        F: Fn(Self::Item) -> Option<T> + Send + Sync + 'static,
        T: Send + 'static;
}

impl<I> ParallelFilterMap for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
    type Item = I::Item;

    fn par_filter_map<T, F>(self, filter_map_op: F) -> ParFilterMap<T>
    where
        F: Fn(Self::Item) -> Option<T> + Send + Sync + 'static,
        T: Send + 'static,
    {
        ParFilterMap::new(self, filter_map_op)
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
