use std::{
    collections::BinaryHeap,
    pin::Pin,
    sync::{Arc, atomic},
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use crate::utils::heap::HeapItem;

pub struct ParMapStream<T> {
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<T>>,
    next: usize,
    rx: tokio::sync::mpsc::Receiver<(usize, T)>,
    start: Option<tokio::sync::oneshot::Sender<bool>>,
    size_hint: (usize, Option<usize>),
}

impl<T> ParMapStream<T>
where
    T: Send + 'static,
{
    #[inline]
    pub(crate) fn new<S, F>(stream: S, map_op: F) -> Self
    where
        S: futures::stream::Stream + Send + 'static,
        S::Item: Send,
        F: FnOnce(S::Item) -> T + Clone + Send + 'static,
    {
        ParMapStream::with_async_fn(stream, move |item| futures::future::ready(map_op(item)))
    }

    #[inline]
    pub(crate) fn with_async_fn<S, F, Fut>(stream: S, map_op: F) -> Self
    where
        S: futures::stream::Stream + Send + 'static,
        S::Item: Send,
        F: FnOnce(S::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = T> + Send,
    {
        let size_hint = stream.size_hint();
        let handle = tokio::runtime::Handle::current();
        let buffer = handle.metrics().num_workers();
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let done = Arc::new(atomic::AtomicBool::new(false));
        handle.spawn({
            let done = done.clone();
            async move {
                if let Ok(true) = start_rx.await {
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
                }
            }
        });
        ParMapStream {
            done,
            heap: BinaryHeap::new(),
            next: 0,
            rx,
            start: Some(start_tx),
            size_hint,
        }
    }
}

impl<T> futures::stream::Stream for ParMapStream<T>
where
    T: Unpin,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(tx) = this.start.take() {
            if tx.send(true).is_err() {
                return Poll::Ready(None);
            }
        }
        if let Some(HeapItem(i, _)) = this.heap.peek() {
            if *i == this.next {
                this.next += 1;
                return Poll::Ready(this.heap.pop().map(|HeapItem(_, item)| item));
            }
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some((i, item))) => {
                if i == this.next {
                    this.next += 1;
                    Poll::Ready(Some(item))
                } else {
                    this.heap.push(HeapItem(i, item));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.size_hint.0.saturating_sub(self.next),
            self.size_hint.1.map(|x| x.saturating_sub(self.next)),
        )
    }
}

impl<T> Drop for ParMapStream<T> {
    fn drop(&mut self) {
        if let Some(tx) = self.start.take() {
            let _ = tx.send(false);
        } else {
            self.done.store(true, atomic::Ordering::Relaxed);
        }
    }
}

pub trait ParallelMapStream {
    type Item;

    fn par_map<T, F>(self, map_op: F) -> ParMapStream<T>
    where
        F: FnOnce(Self::Item) -> T + Clone + Send + 'static,
        T: Send + 'static;

    fn par_map_async<T, Fut, F>(self, map_op: F) -> ParMapStream<T>
    where
        F: FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Send + 'static;
}

impl<S> ParallelMapStream for S
where
    S: futures::stream::Stream + Send + 'static,
    S::Item: Send,
{
    type Item = S::Item;

    fn par_map<T, F>(self, map_op: F) -> ParMapStream<T>
    where
        F: FnOnce(Self::Item) -> T + Clone + Send + 'static,
        T: Send + 'static,
    {
        ParMapStream::new(self, map_op)
    }

    fn par_map_async<T, Fut, F>(self, map_op: F) -> ParMapStream<T>
    where
        F: FnOnce(Self::Item) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Send + 'static,
    {
        ParMapStream::with_async_fn(self, map_op)
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
