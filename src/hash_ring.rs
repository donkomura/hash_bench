use core::panic;
use std::sync::{Arc, Mutex};

use num_traits;

pub trait HashRingInterface<T> {
    fn insert(&mut self, hash: T);
    fn lookup(&self, hash: T) -> Option<Arc<Mutex<Node<T>>>>;
}

#[derive(Debug)]
pub struct Node<T> {
    value: T,
    prev: Option<Arc<Mutex<Node<T>>>>,
    next: Option<Arc<Mutex<Node<T>>>>,
}

pub struct HashRing<T> {
    head: Option<Arc<Mutex<Node<T>>>>,
    k: u32,
    min: T,
    max: T,
}

impl<
        T: std::fmt::Debug
            + std::fmt::Display
            + PartialOrd
            + PartialEq
            + Copy
            + num_traits::NumOps
            + num_traits::Zero
            + num_traits::FromPrimitive
            + num_traits::One
            + num_traits::PrimInt,
    > HashRingInterface<T> for HashRing<T>
{
    fn insert(&mut self, hash: T) {
        if !HashRing::legal_range(self, hash) {
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
        let head_value: T = {
            if let Some(head_node) = self.head.clone() {
                head_node.try_lock().unwrap().value
            } else {
                num_traits::Zero::zero()
            }
        };
        if hash < head_value {
            self.head = Some(Arc::clone(&new_node));
        }
    }
    fn lookup(&self, hash: T) -> Option<Arc<Mutex<Node<T>>>> {
        let mut current = self.head.clone();
        let head_value: T = {
            if let Some(head_node) = self.head.clone() {
                head_node.try_lock().unwrap().value
            } else {
                num_traits::Zero::zero()
            }
        };

        let mut current_value: T;
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
impl<
        T: std::fmt::Debug
            + std::fmt::Display
            + PartialOrd
            + PartialEq
            + Copy
            + PartialEq
            + num_traits::Zero
            + num_traits::FromPrimitive
            + num_traits::One
            + num_traits::NumOps
            + num_traits::PrimInt,
    > HashRing<T>
{
    pub fn new(k: u32) -> Self {
        Self {
            head: None,
            k: k,
            min: num_traits::Zero::zero(),
            max: num_traits::FromPrimitive::from_i64((1 << k) - 1).unwrap(),
        }
    }
    pub fn print(&self) {
        let nodes = self.to_vec();
        println!("min: {}, max: {}", self.min, self.max);
        println!("{:?}", nodes);
    }
    fn to_vec(&self) -> Vec<T> {
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
    fn legal_range(&self, hash: T) -> bool {
        self.min <= hash && hash <= self.max
    }
    fn distance(&self, a: T, b: T) -> T {
        if a == b {
            return num_traits::Zero::zero();
        } else if a < b {
            return b - a;
        }
        let x: T = num_traits::FromPrimitive::from_i64(2).unwrap();
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
