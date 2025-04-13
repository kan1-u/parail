use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use super::*;

pub struct ParFilterMapStream<T, S, F> {
    stream: ParMapStream<Option<T>, S, F>,
}

impl<T, S, F, Fut> ParFilterMapStream<T, S, F>
where
    T: Send + 'static,
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
    F: FnOnce(S::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Option<T>> + Send,
{
    #[inline]
    pub(crate) fn new(stream: S, filter_map_op: F) -> Self {
        let stream = ParMapStream::new(stream, filter_map_op);
        ParFilterMapStream { stream }
    }
}

impl<T, S, F, Fut> futures::stream::Stream for ParFilterMapStream<T, S, F>
where
    Self: Unpin,
    ParMapStream<Option<T>, S, F>: Unpin,
    T: Send + 'static,
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
    F: FnOnce(S::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Option<T>> + Send,
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

pub trait ParallelFilterMapStream: futures::stream::Stream {
    fn par_filter_map<T, F>(self, filter_map_op: F) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> Option<T> + Clone + Send + 'static,
        T: Unpin + Send + 'static;

    fn par_filter_map_async<T, Fut, F>(
        self,
        filter_map_op: F,
    ) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<T>> + Send,
        T: Unpin + Send + 'static;
}

impl<S> ParallelFilterMapStream for S
where
    S: Unpin + futures::stream::Stream + Send + 'static,
    S::Item: Send,
{
    fn par_filter_map<T, F>(self, filter_map_op: F) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> Option<T> + Clone + Send + 'static,
        T: Unpin + Send + 'static,
    {
        ParFilterMapStream::new(self, async move |item| filter_map_op(item))
    }

    fn par_filter_map_async<T, Fut, F>(
        self,
        filter_map_op: F,
    ) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<T>> + Send,
        T: Unpin + Send + 'static,
    {
        ParFilterMapStream::new(self, filter_map_op)
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
