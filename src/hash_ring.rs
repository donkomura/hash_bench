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
            panic!("hash {} is out of range", hash);
        }
        let new_node = Arc::new(Mutex::new(Node {
            value: hash,
            prev: None,
            next: None,
        }));

        if let Some(found) = self.lookup(hash) {
            let node_ref = Arc::clone(&found);
            let mut node = node_ref.try_lock().unwrap();
            let next_node_ref = node
                .next
                .take()
                .expect(&format!("Node {} is found, but it is invalid node", hash));
            node.next = Some(Arc::clone(&new_node));
            drop(node);
            let next_node_ref_clone = Arc::clone(&next_node_ref);
            let mut new_node_mut = new_node.try_lock().unwrap();
            new_node_mut.prev = Some(Arc::clone(&node_ref));
            new_node_mut.next = Some(Arc::clone(&next_node_ref));

            let mut next_node = next_node_ref_clone.try_lock().unwrap();
            next_node.prev = Some(Arc::clone(&new_node));
        } else {
            if let Some(head_ref) = &self.head {
                // head がある場合は head の前（一番後ろ）に挿入する
                let head_prev_ref_clone = {
                    let head = head_ref.try_lock().unwrap();
                    head.next.clone()
                };
                if let Some(head_prev_ref) = head_prev_ref_clone {
                    {
                        let mut head_prev = head_prev_ref.try_lock().unwrap();
                        let mut new_node_mut = new_node.try_lock().unwrap();
                        head_prev.next = Some(Arc::clone(&new_node));
                        new_node_mut.prev = Some(Arc::clone(&head_prev_ref));
                        new_node_mut.next = Some(Arc::clone(&head_ref));
                    }
                    let mut head = head_ref.try_lock().unwrap();
                    head.prev = Some(Arc::clone(&new_node));
                } else {
                    panic!("head.next is None");
                }
            } else {
                // head がない場合はそのまま head に設定する
                self.head = Some(Arc::clone(&new_node));
                let mut head_mut = self.head.as_ref().unwrap().try_lock().unwrap();
                head_mut.next = Some(Arc::clone(&new_node));
                head_mut.prev = Some(Arc::clone(&new_node));
            }
        }
        let head_value: i64 = {
            if let Some(head_node) = self.head.clone() {
                head_node.try_lock().unwrap().value
            } else {
                0
            }
        };
        if hash < head_value {
            self.head = Some(Arc::clone(&new_node));
        }
    }
    fn lookup(&self, hash: i64) -> Option<Arc<Mutex<Node>>> {
        let mut current = self.head.clone();
        let head_value: i64 = {
            if let Some(head_node) = self.head.clone() {
                head_node.try_lock().unwrap().value
            } else {
                0
            }
        };

        let mut current_value: i64;
        while let Some(node) = &current {
            let next_node = {
                let node = node.try_lock().unwrap();
                current_value = node.value;
                node.next.clone()
            };
            if let Some(next) = next_node.clone() {
                if current_value == hash {
                    break;
                }

                let next_node = next.try_lock().unwrap();
                if next_node.value == head_value {
                    break;
                }
                if self.distance(current_value, hash) <= self.distance(next_node.value, hash) {
                    break;
                }
            }
            current = next_node;
        }
        current
    }
}
impl HashRing {
    pub fn new(k: u32) -> Self {
        Self {
            head: None,
            k: k,
            min: 0,
            max: (1 << k) - 1,
        }
    }
    pub fn print(&self) {
        let nodes = self.to_vec();
        println!("min: {}, max: {}", self.min, self.max);
        println!("{:?}", nodes);
    }
    fn to_vec(&self) -> Vec<i64> {
        let mut head = self.head.clone();
        let mut nodes = Vec::new();
        loop {
            let found = nodes.iter().find(|&x| {
                if let Some(ref head_node) = head {
                    let head_value = head_node.try_lock().unwrap().value;
                    *x == head_value
                } else {
                    false
                }
            });
            if found.is_some() {
                break;
            }
            if let Some(node_ref) = head.clone() {
                let node = node_ref.try_lock().unwrap();
                nodes.push(node.value);
                head = node.next.clone();
            } else {
                break;
            }
        }
        nodes
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
        let h = HashRing::new(6);
        assert_eq!(h.distance(0, 5), 5);
        assert_eq!(h.distance(5, 12), 7);
        assert_eq!(h.distance(12, 32), 20);
        assert_eq!(h.distance(5, 18), 13);
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
        h.insert(5);
        h.print();
        h.insert(12);
        h.print();
        h.insert(18);
        h.print();
        h.insert(29);
        h.print();
        let lookup_5 = h.lookup(5);
        assert_eq!(lookup_5.is_some(), true);
        if let Some(node) = lookup_5 {
            let node = node.try_lock().unwrap();
            assert_eq!(node.value, 5);
        }
        let want = vec![5, 12, 18, 29];
        let got = h.to_vec();
        assert_eq!(want, got);
    }
}
