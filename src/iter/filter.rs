use std::{
    collections::BinaryHeap,
    sync::{Arc, atomic},
};

use rayon::prelude::*;

use crate::utils::heap::HeapItem;

pub struct ParFilter<I: Iterator, F> {
    iter: Option<I>,
    filter_op: Option<F>,
    rx: Option<std::sync::mpsc::Receiver<(usize, Option<I::Item>)>>,
    done: Arc<atomic::AtomicBool>,
    heap: BinaryHeap<HeapItem<Option<I::Item>>>,
    next: usize,
}

impl<I, F> ParFilter<I, F>
where
    I: Iterator + Send + 'static,
    I::Item: Send,
    F: Fn(&I::Item) -> bool + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(iter: I, filter_op: F) -> Self {
        ParFilter {
            iter: Some(iter),
            filter_op: Some(filter_op),
            rx: None,
            done: Arc::new(atomic::AtomicBool::new(false)),
            heap: BinaryHeap::new(),
            next: 0,
        }
    }

    #[inline]
    fn start(&mut self) {
        if self.iter.is_none() || self.filter_op.is_none() {
            return;
        }
        if let Some(iter) = self.iter.take() {
            if let Some(filter_op) = self.filter_op.take() {
                let buffer = rayon::current_num_threads();
                let (tx, rx) = std::sync::mpsc::sync_channel(buffer);
                self.rx = Some(rx);
                let done = self.done.clone();
                let op = move || {
                    let _ = iter
                        .take_while(|_| !done.load(atomic::Ordering::Relaxed))
                        .enumerate()
                        .par_bridge()
                        .try_for_each_with(tx, |tx, (i, item)| {
                            tx.send((i, filter_op(&item).then(|| item)))
                        });
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

impl<I, F> Iterator for ParFilter<I, F>
where
    I: Iterator + Send + 'static,
    I::Item: Send,
    F: Fn(&I::Item) -> bool + Send + Sync + 'static,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.start();
        if let Some(HeapItem(i, _)) = self.heap.peek() {
            if *i == self.next {
                self.next += 1;
                if let Some(HeapItem(_, item)) = self.heap.pop() {
                    if item.is_some() {
                        return item;
                    } else {
                        return self.next();
                    }
                }
            }
        }
        if let Some(rx) = self.rx.as_ref() {
            while let Ok((i, item)) = rx.recv() {
                if i == self.next {
                    self.next += 1;
                    if item.is_some() {
                        return item;
                    } else {
                        return self.next();
                    }
                } else {
                    self.heap.push(HeapItem(i, item));
                }
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(iter) = self.iter.as_ref() {
            return (0, iter.size_hint().1);
        } else {
            return (0, None);
        }
    }
}

impl<I: Iterator, F> Drop for ParFilter<I, F> {
    fn drop(&mut self) {
        self.done.store(true, atomic::Ordering::Relaxed);
    }
}

pub trait ParallelFilter: Iterator {
    fn par_filter<F>(self, filter_op: F) -> impl Iterator<Item = Self::Item>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + 'static;
}

impl<I> ParallelFilter for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
    fn par_filter<F>(self, filter_op: F) -> impl Iterator<Item = Self::Item>
    where
        F: Fn(&Self::Item) -> bool + Send + Sync + 'static,
    {
        ParFilter::new(self, filter_op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_filter() {
        let v = vec![1, 2, 3, 4, 5];
        let mut iter = v.into_iter().par_filter(|i| i % 2 == 0);
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), None);
    }
}
