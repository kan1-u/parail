use super::*;

pub struct ParFilterMap<T, I, F> {
    iter: ParMap<Option<T>, I, F>,
}

impl<T, I, F> ParFilterMap<T, I, F>
where
    T: Send + 'static,
    I: Iterator + Send + 'static,
    I::Item: Send,
    F: Fn(I::Item) -> Option<T> + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(iter: I, filter_map_op: F) -> Self {
        let iter = ParMap::new(iter, filter_map_op);
        ParFilterMap { iter }
    }
}

impl<T, I, F> Iterator for ParFilterMap<T, I, F>
where
    T: Send + 'static,
    I: Iterator + Send + 'static,
    I::Item: Send,
    F: Fn(I::Item) -> Option<T> + Send + Sync + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find_map(|item| item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.iter.size_hint().1)
    }
}

pub trait ParallelFilterMap: Iterator {
    fn par_filter_map<T, F>(self, filter_map_op: F) -> impl Iterator<Item = T>
    where
        F: Fn(Self::Item) -> Option<T> + Send + Sync + 'static,
        T: Send + 'static;
}

impl<I> ParallelFilterMap for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
    fn par_filter_map<T, F>(self, filter_map_op: F) -> impl Iterator<Item = T>
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
