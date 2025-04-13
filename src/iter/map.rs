use std::{
    collections::BinaryHeap,
    sync::{Arc, atomic},
};

use rayon::prelude::*;

use crate::utils::heap::HeapItem;

pub struct ParMap<T, I, F> {
    iter: Option<I>,
    map_op: Option<F>,
    rx: Option<std::sync::mpsc::Receiver<(usize, T)>>,
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<T>>,
    next: usize,
}

impl<T, I, F> ParMap<T, I, F>
where
    T: Send + 'static,
    I: Iterator + Send + 'static,
    I::Item: Send,
    F: Fn(I::Item) -> T + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(iter: I, map_op: F) -> Self {
        ParMap {
            iter: Some(iter),
            map_op: Some(map_op),
            rx: None,
            done: Arc::new(atomic::AtomicBool::new(false)),
            heap: BinaryHeap::new(),
            next: 0,
        }
    }

    #[inline]
    fn start(&mut self) {
        if self.iter.is_none() || self.map_op.is_none() {
            return;
        }
        if let Some(iter) = self.iter.take() {
            if let Some(map_op) = self.map_op.take() {
                let buffer = rayon::current_num_threads();
                let (tx, rx) = std::sync::mpsc::sync_channel(buffer);
                self.rx = Some(rx);
                let done = self.done.clone();
                let op = move || {
                    let _ = iter
                        .take_while(|_| !done.load(atomic::Ordering::Relaxed))
                        .enumerate()
                        .par_bridge()
                        .try_for_each_with(tx, |tx, (i, item)| tx.send((i, map_op(item))));
                };
                if let Ok(pool) = rayon::ThreadPoolBuilder::new().num_threads(buffer).build() {
                    pool.spawn(op);
                } else {
                    rayon::spawn(op);
                }
            }
        }
    }
}

impl<T, I, F> Iterator for ParMap<T, I, F>
where
    T: Send + 'static,
    I: Iterator + Send + 'static,
    I::Item: Send,
    F: Fn(I::Item) -> T + Send + Sync + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.start();
        if let Some(HeapItem(i, _)) = self.heap.peek() {
            if *i == self.next {
                self.next += 1;
                return self.heap.pop().map(|HeapItem(_, item)| item);
            }
        }
        if let Some(rx) = self.rx.as_ref() {
            while let Ok((i, item)) = rx.recv() {
                if i == self.next {
                    self.next += 1;
                    return Some(item);
                } else {
                    self.heap.push(HeapItem(i, item));
                }
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(iter) = self.iter.as_ref() {
            return iter.size_hint();
        } else {
            return (0, None);
        }
    }
}

impl<T, I, F> Drop for ParMap<T, I, F> {
    fn drop(&mut self) {
        self.done.store(true, atomic::Ordering::Relaxed);
    }
}

pub trait ParallelMap: Iterator {
    fn par_map<T, F>(self, map_op: F) -> impl Iterator<Item = T>
    where
        F: Fn(Self::Item) -> T + Send + Sync + 'static,
        T: Send + 'static;
}

impl<I> ParallelMap for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
    fn par_map<T, F>(self, map_op: F) -> impl Iterator<Item = T>
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
