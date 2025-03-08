use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use super::*;

pub trait ParallelFilterStream<T> {
    fn par_filter<F>(self, filter_op: F) -> ParFilterStream<T>
    where
        F: FnOnce(&T) -> bool + Clone + Send + 'static;
}

pub struct ParFilterStream<T> {
    iter: ParMapStream<Option<T>>,
}

impl<T> futures::stream::Stream for ParFilterStream<T>
where
    T: Unpin,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.iter.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    if item.is_some() {
                        return Poll::Ready(item);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S, T> ParallelFilterStream<T> for S
where
    S: futures::stream::Stream<Item = T> + Send + 'static,
    T: Send + 'static,
{
    fn par_filter<F>(self, filter_op: F) -> ParFilterStream<T>
    where
        F: FnOnce(&T) -> bool + Clone + Send + 'static,
    {
        let iter = self.par_map(move |item| filter_op(&item).then(|| item));
        ParFilterStream { iter }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_par_filter_stream() {
        let mut iter = futures::stream::iter(0..100).par_filter(|i| i % 2 == 0);
        for i in (0..100).step_by(2) {
            assert_eq!(iter.next().await, Some(i));
        }
    }
}
