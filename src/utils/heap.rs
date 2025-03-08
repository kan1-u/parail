pub struct HeapItem<T>(pub usize, pub T);

impl<T> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        other.0.eq(&self.0)
    }
}

impl<T> Eq for HeapItem<T> {}

impl<T> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}
