use std::{
    collections::BinaryHeap,
    pin::Pin,
    sync::{Arc, atomic},
    task::{Context, Poll},
};

use futures::{Stream, stream::StreamExt};

use crate::utils::heap::HeapItem;

pub struct ParFilterStream<S: Stream, F> {
    stream: Option<S>,
    filter_op: Option<F>,
    rx: Option<tokio::sync::mpsc::Receiver<(usize, Option<S::Item>)>>,
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<Option<S::Item>>>,
    next: usize,
}

impl<S, F, Fut> ParFilterStream<S, F>
where
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
    F: FnOnce(&S::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send,
{
    #[inline]
    pub(crate) fn new(stream: S, filter_op: F) -> Self {
        ParFilterStream {
            stream: Some(stream),
            filter_op: Some(filter_op),
            rx: None,
            done: Arc::new(atomic::AtomicBool::new(false)),
            heap: BinaryHeap::new(),
            next: 0,
        }
    }

    #[inline]
    fn start(&mut self) {
        if self.stream.is_none() || self.filter_op.is_none() {
            return;
        }
        if let Some(stream) = self.stream.take() {
            if let Some(filter_op) = self.filter_op.take() {
                let handle = tokio::runtime::Handle::current();
                let buffer = handle.metrics().num_workers();
                let (tx, rx) = tokio::sync::mpsc::channel(buffer);
                self.rx = Some(rx);
                let done = self.done.clone();
                handle.spawn(async move {
                    let mut this = Box::pin(stream);
                    let mut sets = tokio::task::JoinSet::new();
                    let mut next = 0;
                    while let Some(item) = this.next().await {
                        if done.load(atomic::Ordering::Relaxed) {
                            break;
                        }
                        if sets.len() >= buffer {
                            if let Some(res) = sets.join_next().await {
                                match res {
                                    Ok(Err(_)) | Err(_) => return,
                                    _ => {}
                                }
                                while let Some(res) = sets.try_join_next() {
                                    match res {
                                        Ok(Err(_)) | Err(_) => return,
                                        _ => {}
                                    }
                                }
                            }
                        }
                        let tx = tx.clone();
                        let filter_op = filter_op.clone();
                        sets.spawn(async move {
                            let item = filter_op(&item).await.then(|| item);
                            tx.send((next, item)).await
                        });
                        next += 1;
                    }
                    while let Some(_) = sets.join_next().await {}
                });
            }
        }
    }
}

impl<S, F, Fut> futures::stream::Stream for ParFilterStream<S, F>
where
    Self: Unpin,
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
    F: FnOnce(&S::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.start();
        if let Some(HeapItem(i, _)) = this.heap.peek() {
            if *i == this.next {
                this.next += 1;
                if let Some(item) = this.heap.pop().map(|HeapItem(_, item)| item) {
                    if item.is_some() {
                        return Poll::Ready(item);
                    } else {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }
        }
        if let Some(rx) = this.rx.as_mut() {
            return match rx.poll_recv(cx) {
                Poll::Ready(Some((i, Some(item)))) if i == this.next => {
                    this.next += 1;
                    Poll::Ready(Some(item))
                }
                Poll::Ready(Some((i, None))) if i == this.next => {
                    this.next += 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Some((i, item))) => {
                    this.heap.push(HeapItem(i, item));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
        Poll::Pending
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(stream) = self.stream.as_ref() {
            (0, stream.size_hint().1)
        } else {
            (0, None)
        }
    }
}

pub trait ParallelFilterStream: futures::stream::Stream {
    fn par_filter<F>(self, filter_op: F) -> impl futures::stream::Stream<Item = Self::Item>
    where
        F: Unpin + FnOnce(&Self::Item) -> bool + Clone + Send + 'static;

    fn par_filter_async<Fut, F>(
        self,
        filter_op: F,
    ) -> impl futures::stream::Stream<Item = Self::Item>
    where
        F: Unpin + FnOnce(&Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = bool> + Send;
}

impl<S> ParallelFilterStream for S
where
    S: Unpin + futures::stream::Stream + Send + 'static,
    S::Item: Unpin + Send,
{
    fn par_filter<F>(self, filter_op: F) -> impl futures::stream::Stream<Item = Self::Item>
    where
        F: Unpin + FnOnce(&Self::Item) -> bool + Clone + Send + 'static,
    {
        ParFilterStream::new(self, |item| futures::future::ready(filter_op(item)))
    }

    fn par_filter_async<Fut, F>(
        self,
        filter_op: F,
    ) -> impl futures::stream::Stream<Item = Self::Item>
    where
        F: Unpin + FnOnce(&Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = bool> + Send,
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
