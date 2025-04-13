use std::{
    collections::BinaryHeap,
    pin::Pin,
    sync::{Arc, atomic},
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use crate::utils::heap::HeapItem;

pub struct ParMapStream<T, S, F> {
    stream: Option<S>,
    map_op: Option<F>,
    rx: Option<tokio::sync::mpsc::Receiver<(usize, T)>>,
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<T>>,
    next: usize,
}

impl<T, S, F, Fut> ParMapStream<T, S, F>
where
    T: Send + 'static,
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
    F: FnOnce(S::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = T> + Send,
{
    #[inline]
    pub(crate) fn new(stream: S, map_op: F) -> Self {
        ParMapStream {
            stream: Some(stream),
            map_op: Some(map_op),
            rx: None,
            done: Arc::new(atomic::AtomicBool::new(false)),
            heap: BinaryHeap::new(),
            next: 0,
        }
    }

    #[inline]
    fn start(&mut self) {
        if self.stream.is_none() || self.map_op.is_none() {
            return;
        }
        if let Some(stream) = self.stream.take() {
            if let Some(map_op) = self.map_op.take() {
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
                        let map_op = map_op.clone();
                        sets.spawn(async move {
                            let item = map_op(item).await;
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

impl<T, S, F, Fut> futures::stream::Stream for ParMapStream<T, S, F>
where
    Self: Unpin,
    T: Send + 'static,
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
    F: FnOnce(S::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = T> + Send,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.start();
        if let Some(HeapItem(i, _)) = this.heap.peek() {
            if *i == this.next {
                this.next += 1;
                return Poll::Ready(this.heap.pop().map(|HeapItem(_, item)| item));
            }
        }
        if let Some(rx) = this.rx.as_mut() {
            return match rx.poll_recv(cx) {
                Poll::Ready(Some((i, item))) if i == this.next => {
                    this.next += 1;
                    Poll::Ready(Some(item))
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
            stream.size_hint()
        } else {
            (0, None)
        }
    }
}

impl<T, S, F> Drop for ParMapStream<T, S, F> {
    fn drop(&mut self) {
        self.done.store(true, atomic::Ordering::Relaxed);
    }
}

pub trait ParallelMapStream: futures::stream::Stream + Sized {
    fn par_map<T, F>(self, map_op: F) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> T + Clone + Send + 'static,
        T: Unpin + Send + 'static;

    fn par_map_async<T, Fut, F>(self, map_op: F) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Unpin + Send + 'static;
}

impl<S> ParallelMapStream for S
where
    S: Unpin + futures::stream::Stream + Send + 'static,
    S::Item: Send,
{
    fn par_map<T, F>(self, map_op: F) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> T + Clone + Send + 'static,
        T: Unpin + Send + 'static,
    {
        ParMapStream::new(self, async |item| map_op(item))
    }

    fn par_map_async<T, Fut, F>(self, map_op: F) -> impl futures::stream::Stream<Item = T>
    where
        F: Unpin + FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Unpin + Send + 'static,
    {
        ParMapStream::new(self, map_op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_par_map_stream() {
        let mut iter = futures::stream::iter(0..100).par_map(|i| i * 2);
        for i in 0..100 {
            assert_eq!(iter.next().await, Some(i * 2));
        }
    }
}
