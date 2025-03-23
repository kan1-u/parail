use std::{
    collections::BinaryHeap,
    sync::{Arc, atomic},
};

use rayon::prelude::*;

use crate::utils::{heap::HeapItem, oneshot};

pub struct ParMap<T> {
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<T>>,
    next: usize,
    rx: std::sync::mpsc::Receiver<(usize, T)>,
    start: Option<oneshot::Sender<bool>>,
    size_hint: (usize, Option<usize>),
}

impl<T> ParMap<T>
where
    T: Send + 'static,
{
    #[inline]
    pub(crate) fn new<I, F>(iter: I, map_op: F) -> Self
    where
        I: Iterator + Send + 'static,
        I::Item: Send,
        F: Fn(I::Item) -> T + Send + Sync + 'static,
    {
        let size_hint = iter.size_hint();
        let buffer = rayon::current_num_threads();
        let (tx, rx) = std::sync::mpsc::sync_channel(buffer);
        let (start_tx, start_rx) = oneshot::channel();
        let done = Arc::new(atomic::AtomicBool::new(false));
        rayon::spawn({
            let done = done.clone();
            move || {
                if let Ok(true) = start_rx.recv() {
                    let _ = iter
                        .take_while(|_| !done.load(atomic::Ordering::Relaxed))
                        .enumerate()
                        .par_bridge()
                        .try_for_each_with(tx, |tx, (i, item)| tx.send((i, map_op(item))));
                }
            }
        });
        ParMap {
            done,
            heap: BinaryHeap::new(),
            next: 0,
            rx,
            start: Some(start_tx),
            size_hint,
        }
    }
}

impl<T> Iterator for ParMap<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(tx) = self.start.take() {
            if tx.send(true).is_err() {
                return None;
            }
        }
        if let Some(HeapItem(i, _)) = self.heap.peek() {
            if *i == self.next {
                self.next += 1;
                return self.heap.pop().map(|HeapItem(_, item)| item);
            }
        }
        while let Ok((i, item)) = self.rx.recv() {
            if i == self.next {
                self.next += 1;
                return Some(item);
            } else {
                self.heap.push(HeapItem(i, item));
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.size_hint.0.saturating_sub(self.next),
            self.size_hint.1.map(|x| x.saturating_sub(self.next)),
        )
    }
}

impl<T> Drop for ParMap<T> {
    fn drop(&mut self) {
        if let Some(tx) = self.start.take() {
            let _ = tx.send(false);
        } else {
            self.done.store(true, atomic::Ordering::Relaxed);
        }
    }
}

pub trait ParallelMap {
    type Item;

    fn par_map<T, F>(self, map_op: F) -> ParMap<T>
    where
        F: Fn(Self::Item) -> T + Send + Sync + 'static,
        T: Send + 'static;
}

impl<I> ParallelMap for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
    type Item = I::Item;

    fn par_map<T, F>(self, map_op: F) -> ParMap<T>
    where
        F: Fn(Self::Item) -> T + Send + Sync + 'static,
        T: Send + 'static,
    {
        ParMap::new(self, map_op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_map() {
        let v = vec![1, 2, 3, 4, 5];
        let mut iter = v.into_iter().par_map(|i| i * 2);
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(8));
        assert_eq!(iter.next(), Some(10));
        assert_eq!(iter.next(), None);
    }
}
