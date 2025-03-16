use core::panic;
use num_traits;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait HashRingInterface<T: std::hash::Hash> {
    fn add_node(&mut self, hash: T);
    fn lookup(&self, hash: T) -> Option<Arc<Mutex<Node<T>>>>;
    fn move_resource(&self, dest: T, src: T, is_delete: bool);
    fn add_resource(&self, hash: T);
}

#[derive(Debug)]
pub struct Node<T> {
    value: T,
    resource: HashMap<T, T>,
    prev: Option<Arc<Mutex<Node<T>>>>,
    next: Option<Arc<Mutex<Node<T>>>>,
}

impl<T> Node<T> {
    pub fn value(&self) -> &T {
        &self.value
    }
}

pub struct HashRing<T> {
    head: Option<Arc<Mutex<Node<T>>>>,
    k: u32,
    min: T,
    max: T,
}

impl<
        T: std::fmt::Debug
            + std::hash::Hash
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
    fn add_node(&mut self, hash: T) {
        if !self.legal_range(hash) {
            panic!("hash {} is out of range", hash);
        }
        let new_node = Arc::new(Mutex::new(Node {
            value: hash,
            resource: HashMap::new(),
            prev: None,
            next: None,
        }));

        let next_node_value: T;
        if let Some(ref found) = self.lookup(hash) {
            let node_ref = Arc::clone(found);
            let mut node = node_ref.try_lock().unwrap();
            let prev_node_ref = node
                .prev
                .take()
                .unwrap_or_else(|| panic!("Node {} is found, but it is invalid node", hash));
            node.prev = Some(Arc::clone(&new_node));
            next_node_value = *node.value();
            drop(node);

            let mut new_node_mut = new_node.try_lock().unwrap();
            new_node_mut.prev = Some(Arc::clone(&prev_node_ref));
            new_node_mut.next = Some(Arc::clone(&node_ref));
            drop(new_node_mut);

            let mut prev_node = prev_node_ref.try_lock().unwrap();
            prev_node.next = Some(Arc::clone(&new_node));
        } else if let Some(head_ref) = &self.head {
            // head がある場合は head の前（一番後ろ）に挿入する
            let head_prev_ref_clone = {
                let head = head_ref.try_lock().unwrap();
                head.next.clone()
            };
            if let Some(ref head_prev_ref) = head_prev_ref_clone {
                let mut head_prev = head_prev_ref.try_lock().unwrap();
                let mut new_node_mut = new_node.try_lock().unwrap();
                head_prev.next = Some(Arc::clone(&new_node));
                new_node_mut.prev = Some(Arc::clone(head_prev_ref));
                new_node_mut.next = Some(Arc::clone(head_ref));
                drop(new_node_mut);
                drop(head_prev);

                let mut head = head_ref.try_lock().unwrap();
                head.prev = Some(Arc::clone(&new_node));
                next_node_value = *head.value();
            } else {
                panic!("head.next is None");
            }
        } else {
            // head がない場合はそのまま head に設定する
            self.head = Some(Arc::clone(&new_node));
            let mut head_mut = self.head.as_ref().unwrap().try_lock().unwrap();
            head_mut.next = Some(Arc::clone(&new_node));
            head_mut.prev = Some(Arc::clone(&new_node));
            next_node_value = hash;
        }
        self.head = Some(Arc::clone(&new_node));
        self.move_resource(hash, next_node_value, false);
        println!("add node: {}, and now moving resources...", hash);
    }

    fn lookup(&self, hash: T) -> Option<Arc<Mutex<Node<T>>>> {
        let mut current = self.head.clone();
        let head_value: T = {
            if let Some(head_node) = self.head.clone() {
                *head_node.try_lock().unwrap().value()
            } else {
                num_traits::Zero::zero()
            }
        };

        while let Some(node) = &current {
            let current_value: T;
            let next_node = {
                let node = node.try_lock().unwrap();
                current_value = *node.value();
                node.next.clone()
            };
            if let Some(next) = next_node.clone() {
                let next_node = next.try_lock().unwrap();
                if current_value == hash {
                    break;
                }
                if *next_node.value() == head_value {
                    break;
                }
                if self.distance(hash, current_value) < self.distance(hash, *next_node.value()) {
                    break;
                }
            }
            current = next_node;
        }
        current
    }

    fn move_resource(&self, dest: T, src: T, is_delete: bool) {
        let mut resources: Vec<(T, T)> = Vec::new();
        let dest_node = self.lookup(dest);
        let src_node = self.lookup(src);
        if dest_node.is_none() || src_node.is_none() {
            panic!("dest or src is not found");
        }

        if let Some(src_node_ref) = src_node {
            let mut _src_node = src_node_ref.try_lock().unwrap();
            assert!(src == *_src_node.value());
            for (key, value) in _src_node.resource.iter() {
                if self.distance(*key, dest) < self.distance(*key, src) || is_delete {
                    println!(
                        "{} will move because distance dest: {}, distance src: {}",
                        *key,
                        self.distance(*key, dest),
                        self.distance(*key, src)
                    );
                    resources.push((*key, *value));
                }
            }
            for (key, _) in &resources {
                _src_node.resource.remove(key);
            }
        }

        if let Some(dest_node_ref) = dest_node {
            let mut dest_node = dest_node_ref.try_lock().unwrap();
            assert!(dest == *dest_node.value());
            for (key, value) in resources {
                dest_node.resource.insert(key, value);
            }
        }
    }

    fn add_resource(&self, hash: T) {
        if !self.legal_range(hash) {
            panic!("hash {} is out of range", hash);
        }
        let node_ref = self.lookup(hash);
        if let Some(node) = node_ref {
            let mut node = node.try_lock().unwrap();
            node.resource.insert(hash, hash);
        } else {
            panic!("node is not found");
        }
    }
}

impl<
        T: std::fmt::Debug
            + std::fmt::Display
            + PartialOrd
            + PartialEq
            + Copy
            + std::hash::Hash
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
            k,
            min: num_traits::Zero::zero(),
            max: num_traits::FromPrimitive::from_i64((1 << k) - 1).unwrap(),
        }
    }

    pub fn print(&self) {
        let nodes = self.nodes();
        println!("min: {}, max: {}", self.min, self.max);
        println!("{:?}", nodes);
        let head_value = {
            if let Some(head_node) = self.head.clone() {
                *head_node.try_lock().unwrap().value()
            } else {
                num_traits::Zero::zero()
            }
        };
        println!("head: {:?}", head_value);
        for (key, vec) in self.resources().iter() {
            println!("node: {}, value: {:?}", key, vec);
        }
    }

    fn resources(&self) -> HashMap<T, Vec<(T, T)>> {
        let mut head = self.head.clone();
        let mut resources: HashMap<T, Vec<(T, T)>> = HashMap::new();
        let head_value: T = {
            if let Some(head_node) = self.head.clone() {
                *head_node.try_lock().unwrap().value()
            } else {
                num_traits::Zero::zero()
            }
        };
        while let Some(node_ref) = head.clone() {
            {
                let node = node_ref.try_lock().unwrap();
                let mut resource: Vec<(T, T)> = Vec::new();
                let mut node_resources: Vec<(&T, &T)> = node.resource.iter().collect();
                node_resources.sort_by(|a, b| a.0.cmp(b.0));
                for (key, value) in node_resources {
                    resource.push((*key, *value));
                }
                resources.insert(*node.value(), resource);
                head = node.next.clone();
            }

            if let Some(node_ref) = head.clone() {
                let node = node_ref.try_lock().unwrap();
                if *node.value() == head_value {
                    break;
                }
            } else {
                break;
            }
        }
        resources
    }

    fn nodes(&self) -> Vec<T> {
        let mut head = self.head.clone();
        let mut nodes = Vec::new();
        while let Some(node_ref) = head.clone() {
            {
                let node = node_ref.try_lock().unwrap();
                nodes.push(*node.value());
                head = node.next.clone();
            }

            let found = nodes.iter().find(|&x| {
                if let Some(ref head_node) = head {
                    let head_value = *head_node.try_lock().unwrap().value();
                    *x == head_value
                } else {
                    false
                }
            });
            if found.is_some() {
                break;
            }
        }
        nodes
    }

    fn legal_range(&self, hash: T) -> bool {
        self.min <= hash && hash <= self.max
    }

    fn distance(&self, a: T, b: T) -> T {
        match a.cmp(&b) {
            std::cmp::Ordering::Equal => num_traits::Zero::zero(),
            std::cmp::Ordering::Less => b - a,
            std::cmp::Ordering::Greater => {
                let x: T = num_traits::FromPrimitive::from_i64(2).unwrap();
                x.pow(self.k) + (b - a)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn distance_ring_5() {
        let h = HashRing::new(5);
        assert_eq!(h.distance(0, 5), 5);
        assert_eq!(h.distance(5, 12), 7);
        assert_eq!(h.distance(12, 32), 20);
        assert_eq!(h.distance(5, 18), 13);
        assert_eq!(h.distance(29, 5), 8);
        assert_eq!(h.distance(5, 5), 0);
        assert_eq!(h.distance(29, 12), 15);
        assert_eq!(h.distance(5, 29), 24);
        assert_eq!(h.distance(12, 24), 12);
        assert_eq!(h.distance(18, 24), 6);
    }

    #[test]
    fn hash_ring_add_node_lookup() {
        let mut h = HashRing::new(5);
        h.add_node(3);
        let _node_ref = h.lookup(3);
    }

    #[test]
    fn multiple_add_node_lookup() {
        let mut h = HashRing::new(5);
        h.add_node(5);
        h.print();
        h.add_node(12);
        h.print();
        h.add_node(18);
        h.print();
        h.add_node(29);
        h.print();
        let lookup_5 = h.lookup(5);
        assert!(lookup_5.is_some());
        if let Some(node) = lookup_5 {
            let node = node.try_lock().unwrap();
            assert_eq!(*node.value(), 5);
        }
        let want = vec![29, 5, 12, 18];
        let got = h.nodes();
        assert_eq!(want, got);
    }

    #[test]
    fn add_resource() {
        let mut h = HashRing::new(5);
        h.add_node(12);
        h.add_node(18);
        h.add_resource(24);
        h.add_resource(21);
        h.add_resource(16);
        h.add_resource(23);
        h.add_resource(2);
        h.add_resource(29);
        h.add_resource(28);
        h.add_resource(7);
        h.add_resource(10);
        h.print();
        assert_eq!(h.resources().len(), 2);
        assert_eq!(h.resources().get(&12).unwrap().len(), 8);
        assert_eq!(h.resources().get(&18).unwrap().len(), 1);
        assert_eq!(h.resources().get(&18), Some(&vec![(16, 16)]));
        assert_eq!(
            h.resources().get(&12),
            Some(&vec![
                (2, 2),
                (7, 7),
                (10, 10),
                (21, 21),
                (23, 23),
                (24, 24),
                (28, 28),
                (29, 29)
            ])
        );
    }

    #[test]
    fn move_resource() {
        let mut h = HashRing::new(5);
        h.add_node(12);
        h.add_node(18);
        h.print();
        h.add_resource(24);
        h.add_resource(21);
        h.add_resource(16);
        h.add_resource(23);
        h.add_resource(2);
        h.add_resource(29);
        h.add_resource(28);
        h.add_resource(7);
        h.add_resource(10);
        h.print();
        h.move_resource(12, 18, true);
        h.print();
        assert_eq!(h.resources().get(&18).unwrap().len(), 0);
        assert_eq!(h.resources().get(&12).unwrap().len(), 9);
        assert_eq!(h.resources().get(&18), Some(&vec![]));
        assert_eq!(
            h.resources().get(&12),
            Some(&vec![
                (2, 2),
                (7, 7),
                (10, 10),
                (16, 16),
                (21, 21),
                (23, 23),
                (24, 24),
                (28, 28),
                (29, 29)
            ])
        );
    }

    #[test]
    fn add_resource_with_resource_move() {
        let mut h = HashRing::new(5);
        h.add_node(12);
        h.add_node(18);
        h.add_resource(24);
        h.add_resource(21);
        h.add_resource(16);
        h.add_resource(23);
        h.add_resource(2);
        h.add_resource(29);
        h.add_resource(28);
        h.add_resource(7);
        h.add_resource(10);
        h.print();
        assert_eq!(h.resources().get(&18).unwrap().len(), 1);
        assert_eq!(h.resources().get(&12).unwrap().len(), 8);
        h.add_node(5);
        h.print();
        h.add_node(27);
        h.print();
        h.add_node(30);
        h.print();
        assert_eq!(h.resources().get(&5).unwrap().len(), 1);
        assert_eq!(h.resources().get(&12).unwrap().len(), 2);
        assert_eq!(h.resources().get(&18).unwrap().len(), 1);
        assert_eq!(h.resources().get(&27).unwrap().len(), 3);
        assert_eq!(h.resources().get(&30).unwrap().len(), 2);
        assert_eq!(h.resources().get(&5), Some(&vec![(2, 2)]));
        assert_eq!(h.resources().get(&12), Some(&vec![(7, 7), (10, 10)]));
        assert_eq!(h.resources().get(&18), Some(&vec![(16, 16)]));
        assert_eq!(
            h.resources().get(&27),
            Some(&vec![(21, 21), (23, 23), (24, 24)])
        );
        assert_eq!(h.resources().get(&30), Some(&vec![(28, 28), (29, 29)]));
        assert_eq!(h.resources().get(&18), Some(&vec![(16, 16)]));
    }
}
