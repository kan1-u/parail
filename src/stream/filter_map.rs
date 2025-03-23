use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use super::*;

pub struct ParFilterMapStream<T> {
    stream: ParMapStream<Option<T>>,
}

impl<T> ParFilterMapStream<T>
where
    T: Send + 'static,
{
    #[inline]
    pub(crate) fn new<S, F>(stream: S, filter_map_op: F) -> Self
    where
        S: futures::stream::Stream + Send + 'static,
        S::Item: Send,
        F: FnOnce(S::Item) -> Option<T> + Clone + Send + 'static,
    {
        let stream = ParMapStream::new(stream, filter_map_op);
        ParFilterMapStream { stream }
    }

    #[inline]
    pub(crate) fn with_async_fn<S, F, Fut>(stream: S, filter_map_op: F) -> Self
    where
        S: futures::stream::Stream + Send + 'static,
        S::Item: Send,
        F: FnOnce(S::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<T>> + Send,
    {
        let stream = ParMapStream::with_async_fn(stream, filter_map_op);
        ParFilterMapStream { stream }
    }
}

impl<T> futures::stream::Stream for ParFilterMapStream<T>
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

pub trait ParallelFilterMapStream {
    type Item;

    fn par_filter_map<T, F>(self, filter_map_op: F) -> ParFilterMapStream<T>
    where
        F: FnOnce(Self::Item) -> Option<T> + Clone + Send + 'static,
        T: Send + 'static;

    fn par_filter_map_async<T, Fut, F>(self, filter_map_op: F) -> ParFilterMapStream<T>
    where
        F: FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<T>> + Send,
        T: Send + 'static;
}

impl<S> ParallelFilterMapStream for S
where
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
{
    type Item = S::Item;

    fn par_filter_map<T, F>(self, filter_map_op: F) -> ParFilterMapStream<T>
    where
        F: FnOnce(Self::Item) -> Option<T> + Clone + Send + 'static,
        T: Send + 'static,
    {
        ParFilterMapStream::new(self, filter_map_op)
    }

    fn par_filter_map_async<T, Fut, F>(self, filter_map_op: F) -> ParFilterMapStream<T>
    where
        F: FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<T>> + Send,
        T: Send + 'static,
    {
        ParFilterMapStream::with_async_fn(self, filter_map_op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_par_filter_map_stream() {
        let mut stream = futures::stream::iter(0..100)
            .par_filter_map(|i| if i % 2 == 0 { Some(i) } else { None });
        for i in (0..100).step_by(2) {
            assert_eq!(stream.next().await, Some(i));
        }
    }
}
