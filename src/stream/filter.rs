use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use super::*;

pub struct ParFilterStream<T> {
    stream: ParMapStream<Option<T>>,
}

impl<T> ParFilterStream<T>
where
    T: Send + 'static,
{
    #[inline]
    pub(crate) fn new<S, F>(stream: S, filter_op: F) -> Self
    where
        S: futures::stream::Stream<Item = T> + Send + 'static,
        F: FnOnce(&S::Item) -> bool + Clone + Send + 'static,
    {
        let stream = ParMapStream::new(stream, move |item| filter_op(&item).then(|| item));
        ParFilterStream { stream }
    }
}

impl<T> futures::stream::Stream for ParFilterStream<T>
where
    T: Unpin,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.get_mut().stream.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                if item.is_some() {
                    Poll::Ready(item)
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.stream.size_hint().1)
    }
}

pub trait ParallelFilterStream {
    type Item;

    fn par_filter<F>(self, filter_op: F) -> ParFilterStream<Self::Item>
    where
        F: FnOnce(&Self::Item) -> bool + Clone + Send + 'static;
}

impl<S> ParallelFilterStream for S
where
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
{
    type Item = S::Item;

    fn par_filter<F>(self, filter_op: F) -> ParFilterStream<Self::Item>
    where
        F: FnOnce(&Self::Item) -> bool + Clone + Send + 'static,
    {
        ParFilterStream::new(self, filter_op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_par_filter_stream() {
        let mut stream = futures::stream::iter(0..100).par_filter(|i| i % 2 == 0);
        for i in (0..100).step_by(2) {
            assert_eq!(stream.next().await, Some(i));
        }
    }
}
