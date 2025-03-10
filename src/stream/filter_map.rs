use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use super::*;

pub trait ParallelFilterMapStream<T>: Sized {
    fn par_filter_map<F, R>(self, filter_map_op: F) -> ParFilterMapStream<R>
    where
        F: FnOnce(T) -> Option<R> + Clone + Send + 'static,
        R: Send + 'static,
    {
        self.par_filter_map_async(move |item| futures::future::ready(filter_map_op(item)))
    }

    fn par_filter_map_async<F, Fut, R>(self, filter_map_op: F) -> ParFilterMapStream<R>
    where
        F: FnOnce(T) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<R>> + Send,
        R: Send + 'static;
}

pub struct ParFilterMapStream<T> {
    stream: ParMapStream<Option<T>>,
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
}

impl<S, T> ParallelFilterMapStream<T> for S
where
    S: futures::stream::Stream<Item = T> + Send + 'static,
    T: Send + 'static,
{
    fn par_filter_map_async<F, Fut, R>(self, filter_map_op: F) -> ParFilterMapStream<R>
    where
        F: FnOnce(T) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<R>> + Send,
        R: Send + 'static,
    {
        let stream = self.par_map_async(filter_map_op);
        ParFilterMapStream { stream }
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
