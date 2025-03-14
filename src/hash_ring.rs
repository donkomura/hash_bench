pub trait HashRingInterface<T: std::fmt::Debug> {
    fn new() -> Self;
    fn insert(&mut self, element: T);
    fn delete(&mut self);
}

struct Node<T> {
    value: T,
    next: Node<T>;
}

#[derive(Debug)]
struct HashRing<T: std::fmt::Debug> {
    head: Node<T>,
    k: u64,
    min: 0,
    max: 1 << k - 1,
}

