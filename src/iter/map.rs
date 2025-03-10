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

pub trait ParallelMap<T> {
    fn par_map<F, R>(self, map_op: F) -> ParMap<R>
    where
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Send + 'static;
}

impl<T, I> ParallelMap<T> for I
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send,
{
    fn par_map<F, R>(self, map_op: F) -> ParMap<R>
    where
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let (start_tx, start_rx) = oneshot::channel();
        let done = Arc::new(atomic::AtomicBool::new(false));
        let op = {
            let done = done.clone();
            move || {
                if let Ok(true) = start_rx.recv() {
                    let _ = self
                        .take_while(|_| !done.load(atomic::Ordering::Relaxed))
                        .enumerate()
                        .par_bridge()
                        .map(move |(i, item)| (i, map_op(item)))
                        .try_for_each_with(tx, |tx, item| tx.send(item));
                }
            }
        };
        if let Ok(pool) = rayon::ThreadPoolBuilder::new().build() {
            pool.spawn(op);
        } else {
            std::thread::spawn(op);
        }
        ParMap {
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
