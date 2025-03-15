use core::panic;
use std::sync::{Arc, Mutex};

pub trait HashRingInterface {
    fn insert(&mut self, hash: i64);
    fn lookup(&self, hash: i64) -> Option<Arc<Mutex<Node>>>;
}

#[derive(Debug)]
pub struct Node {
    value: i64,
    prev: Option<Arc<Mutex<Node>>>,
    next: Option<Arc<Mutex<Node>>>,
}

pub struct HashRing {
    head: Option<Arc<Mutex<Node>>>,
    k: u32,
    min: i64,
    max: i64,
}

impl HashRingInterface for HashRing {
    fn insert(&mut self, hash: i64) {
        if !self.leagal_range(hash) {
            return;
        }
        let new_node = Arc::new(Mutex::new(Node {
            value: hash,
            prev: None,
            next: None,
        }));

        if let Some(found) = self.lookup(hash) {
            let node_ref = Arc::clone(&found);
            let mut node = node_ref.try_lock().unwrap();
            if let Some(next_node_ref) = node.next.take() {
                let mut next_node= next_node_ref.try_lock().unwrap();
                next_node.prev = Some(Arc::clone(&new_node));
                let mut new_node_mut = new_node.try_lock().unwrap();
                new_node_mut.prev = Some(Arc::clone(&node_ref));
                new_node_mut.next = Some(Arc::clone(&next_node_ref));
            } else {
                panic!("Node {} is found, but it is invalid node", hash);
            }
            node.prev = Some(new_node);
        } else {
            if let Some(head_ref) = &self.head {
                // head がある場合は head の後ろに挿入する
                let tail_node_ref = {
                    let head = head_ref.try_lock().unwrap();
                    head.next.clone()
                };
                if let Some(tail_node_ref) = tail_node_ref {
                    {
                        let mut tail = tail_node_ref.try_lock().unwrap();
                        let mut new_node_mut = new_node.try_lock().unwrap();
                        tail.prev = Some(Arc::clone(&new_node));
                        new_node_mut.next = Some(Arc::clone(&tail_node_ref));
                        new_node_mut.prev = Some(Arc::clone(&head_ref));
                    }

                    let mut head = head_ref.try_lock().unwrap();
                    head.next = Some(Arc::clone(&new_node));
                } else {
                    let mut head = head_ref.try_lock().unwrap();
                    head.next = Some(Arc::clone(&new_node));
                    let mut new_node_mut = new_node.try_lock().unwrap();
                    new_node_mut.next = Some(Arc::clone(&head_ref));
                    new_node_mut.prev = Some(Arc::clone(&head_ref));
                }
            } else {
                // head がない場合はそのまま head に設定する
                self.head = Some(Arc::clone(&new_node));
                let mut new_node_mut = new_node.try_lock().unwrap();
                new_node_mut.next = Some(Arc::clone(&new_node));
                new_node_mut.prev = Some(Arc::clone(&new_node));
            }
        }
    }
    fn lookup(&self, hash: i64) -> Option<Arc<Mutex<Node>>> {
        let mut current = self.head.clone();
        let head_value: i64;
        if let Some(head_node) = self.head.clone() {
            head_value = head_node.try_lock().unwrap().value;
        } else {
            return None;
        }

        let mut current_value: i64;
        while let Some(node) = current {
            let node_ref = Arc::clone(&node);
            {
                let node = node.try_lock().unwrap();
                current_value = node.value;
                if node.value == hash {
                    return Some(node_ref);
                }
                current = node.next.clone();
            }
            if let Some(next) = current.clone() {
                let next_node = next.try_lock().unwrap();
                if next_node.value == head_value {
                    break;
                }
                if self.distance(current_value, hash) > self.distance(next_node.value, hash) {
                    break;
                }
            }
        }
        if let Some(current_node) = current {
            if current_node.try_lock().unwrap().value == hash {
                return Some(current_node);
            }
        }
        None
    }
}
impl HashRing {
    pub fn new(k: u32) -> Self {
        Self {
            head: None,
            k: k,
            min: 0,
            max: 1 << k - 1,
        }
    }
    fn leagal_range(&self, hash: i64) -> bool {
        self.min <= hash && hash <= self.max
    }
    fn distance(&self, a: i64, b: i64) -> i64 {
        if a == b {
            return 0;
        } else if a < b {
            return b - a;
        }
        let x: i64 = 2;
        return x.pow(self.k) + (b - a);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn distance_ring_5() {
        let h = HashRing::new(5);
        assert_eq!(h.distance(29, 5), 8);
        assert_eq!(h.distance(29, 12), 15);
        assert_eq!(h.distance(5, 29), 24);
    }
    #[test]
    fn hash_ring_insert_lookup() {
        let mut h = HashRing::new(5);
        h.insert(3);
        let _node_ref = h.lookup(3);
    }

    #[test]
    fn multiple_insert_lookup() {
        let mut h = HashRing::new(5);
        h.insert(3);
        h.insert(5);
        h.insert(12);
        let lookup_5 = h.lookup(5);
        assert_eq!(lookup_5.is_some(), true);
        if let Some(node) = lookup_5{
            let node = node.try_lock().unwrap();
            assert_eq!(node.value, 5);
        }
        let lookup_100 = h.lookup(100);
        assert_eq!(lookup_100.is_some(), false);
    }
}
