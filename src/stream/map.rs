use std::{
    collections::BinaryHeap,
    pin::Pin,
    sync::{Arc, atomic},
    task::{Context, Poll},
};

use futures::stream::StreamExt;

use crate::utils::heap::HeapItem;

pub trait ParallelMapStream<T>: Sized {
    fn par_map<F, R>(self, map_op: F) -> ParMapStream<R>
    where
        F: FnOnce(T) -> R + Clone + Send + 'static,
        R: Send + 'static,
    {
        self.par_map_async(move |item| futures::future::ready(map_op(item)))
    }

    fn par_map_async<F, Fut, R>(self, map_op: F) -> ParMapStream<R>
    where
        F: FnOnce(T) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = R> + Send,
        R: Send + 'static;
}

pub struct ParMapStream<T> {
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<T>>,
    next: usize,
    rx: tokio::sync::mpsc::Receiver<(usize, T)>,
    start: Option<tokio::sync::oneshot::Sender<bool>>,
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

impl<S, T> ParallelMapStream<T> for S
where
    S: futures::stream::Stream<Item = T> + Send + 'static,
    T: Send + 'static,
{
    fn par_map_async<F, Fut, R>(self, map_op: F) -> ParMapStream<R>
    where
        F: FnOnce(T) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = R> + Send,
        R: Send + 'static,
    {
        let handle = tokio::runtime::Handle::current();
        let buffer = handle.metrics().num_workers() * 2;
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let done = Arc::new(atomic::AtomicBool::new(false));
        handle.spawn({
            let done = done.clone();
            async move {
                if let Ok(true) = start_rx.await {
                    let mut this = Box::pin(self);
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
        }
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
