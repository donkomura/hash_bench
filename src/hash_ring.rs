use core::panic;
use log::{info, warn};
use num_traits;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait HashRingInterface<T: std::hash::Hash> {
    fn add_node(&mut self, hash: T);
    fn remove_node(&mut self, hash: T);
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
        if let Some(ref found) = self.lookup(hash).clone() {
            // すでにノードが存在する場合はその前に挿入する
            self.add_node_prev(found, &new_node);
            next_node_value = self.get_node_value(&Some(found.clone()));
        } else if let Some(ref head_ref) = &self.head.clone() {
            // head がある場合は head の前（一番後ろ）に挿入する
            self.add_node_prev(head_ref, &new_node);
            next_node_value = self.get_node_value(&Some(head_ref.clone()));
        } else {
            // head がない場合はそのまま head に設定する
            self.head = Some(Arc::clone(&new_node));
            let mut head_mut = self.head.as_ref().unwrap().try_lock().unwrap();
            head_mut.next = Some(Arc::clone(&new_node));
            head_mut.prev = Some(Arc::clone(&new_node));
            next_node_value = hash;
        }
        info!("add node: {}, and now moving resources...", hash);
        self.move_resource(hash, next_node_value, false);
        let head_value = self.get_head_value();
        if hash < head_value {
            self.head = Some(Arc::clone(&new_node));
        }
    }

    fn remove_node(&mut self, hash: T) {
        let node_ref = self.lookup(hash);
        let node_value = self.get_node_value(&node_ref);
        let next_value = self.get_next_value(&node_ref);
        if node_value != hash {
            warn!("node {} is not found, skip removing", hash);
            return;
        }
        info!(
            "remove node: {}, and now moving resources to {}...",
            node_value, next_value
        );
        self.move_resource(next_value, node_value, true);

        let head_value = self.get_head_value();
        let head_next_value = self.get_next_value(&self.head.clone());
        let prev_node_ref = self.get_prev_node_ref(&node_ref);
        let next_node_ref = self.get_next_node_ref(&node_ref);
        if let Some(prev_node) = &prev_node_ref {
            let mut prev = prev_node.try_lock().unwrap();
            prev.next = next_node_ref.clone();
        }
        if let Some(next_node) = &next_node_ref {
            let mut next = next_node.try_lock().unwrap();
            next.prev = prev_node_ref.clone();
        }
        if head_value == head_next_value {
            self.head = next_node_ref.clone();
            if head_value == hash {
                self.head = None;
            }
        }
    }

    fn lookup(&self, hash: T) -> Option<Arc<Mutex<Node<T>>>> {
        let mut current = self.head.clone();
        let mut current_value: T = self.get_node_value(&current);
        let mut next_node_ref = self.get_next_node_ref(&current);
        let mut next_node_value = self.get_node_value(&next_node_ref);
        let head_value: T = self.get_head_value();

        while self.distance(current_value, hash) > self.distance(next_node_value, hash) {
            info!(
                "looking for hash: {}, current: {}, next: {}",
                hash, current_value, next_node_value
            );
            if current_value == hash {
                break;
            }
            if next_node_value == head_value {
                break;
            }
            current = next_node_ref;
            current_value = self.get_node_value(&current);
            next_node_ref = self.get_next_node_ref(&current);
            next_node_value = self.get_node_value(&next_node_ref);
        }
        info!("hash {} found in node {}", hash, current_value);
        if current_value == hash {
            return current;
        }
        next_node_ref
    }

    fn move_resource(&self, dest: T, src: T, is_delete: bool) {
        let mut resources: Vec<(T, T)> = Vec::new();
        let dest_node = self.lookup(dest);
        let src_node = self.lookup(src);
        if dest_node.is_none() || src_node.is_none() {
            panic!("dest {} or src {} is not found", dest, src);
        }

        if let Some(src_node_ref) = src_node {
            let mut _src_node = src_node_ref.try_lock().unwrap();
            assert!(src == *_src_node.value());
            for (key, value) in _src_node.resource.iter() {
                if self.distance(*key, dest) < self.distance(*key, src) || is_delete {
                    info!(
                        "{} will move because distance dest {}: {}, distance src {}: {}",
                        *key,
                        dest,
                        self.distance(*key, dest),
                        src,
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

            info!("add resource {} to node {}", hash, node.value);
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

    fn add_node_prev(&mut self, target: &Arc<Mutex<Node<T>>>, new_node: &Arc<Mutex<Node<T>>>) {
        let prev_node_ref = {
            let mut node = target.try_lock().unwrap();
            let prev = node
                .prev
                .clone()
                .expect("Node is found, but it is an invalid node: prev does not set");
        node.prev = Some(Arc::clone(new_node));
            prev
        };
        {
        let mut new_node_mut = new_node.try_lock().unwrap();
            new_node_mut.prev = Some(Arc::clone(&prev_node_ref));
        new_node_mut.next = Some(Arc::clone(target));
        }
        {
        let mut prev_node = prev_node_ref.try_lock().unwrap();
        prev_node.next = Some(Arc::clone(new_node));
        }
    }
    fn get_head_value(&self) -> T {
        self.get_node_value(&self.head)
    }
    fn get_node_value(&self, node_ref: &Option<Arc<Mutex<Node<T>>>>) -> T {
        if let Some(node_ref) = node_ref {
            return *node_ref.try_lock().unwrap().value();
        }
        num_traits::Zero::zero()
    }
    fn get_next_value(&self, node_ref: &Option<Arc<Mutex<Node<T>>>>) -> T {
        if let Some(next_node_ref) = self.get_next_node_ref(node_ref) {
            let next = next_node_ref.try_lock().unwrap();
            return *next.value();
        }
        num_traits::Zero::zero()
    }
    fn get_next_node_ref(
        &self,
        node_ref: &Option<Arc<Mutex<Node<T>>>>,
    ) -> Option<Arc<Mutex<Node<T>>>> {
        if let Some(node_ref) = node_ref {
            let node = node_ref.try_lock().unwrap();
            return node.next.clone();
        }
        None
    }
    fn get_prev_node_ref(
        &self,
        node_ref: &Option<Arc<Mutex<Node<T>>>>,
    ) -> Option<Arc<Mutex<Node<T>>>> {
        if let Some(node_ref) = node_ref {
            let node = node_ref.try_lock().unwrap();
            return node.prev.clone();
        }
        None
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
    use crate::log;

    #[test]
    fn distance_ring_5() {
        log::init_test_logger();
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
        log::init_test_logger();
        let mut h = HashRing::new(5);
        h.add_node(3);
        let _node_ref = h.lookup(3);
    }

    #[test]
    fn multiple_add_node_lookup() {
        log::init_test_logger();
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
        let want = vec![5, 12, 18, 29];
        let got = h.nodes();
        assert_eq!(want, got);
    }

    #[test]
    fn add_resource() {
        log::init_test_logger();
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
        log::init_test_logger();
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
        log::init_test_logger();
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

        h.remove_node(12);
        h.print();
        assert_eq!(h.resources().get(&5).unwrap().len(), 1);
        assert_eq!(h.resources().get(&18).unwrap().len(), 3);
        assert_eq!(h.resources().get(&27).unwrap().len(), 3);
        assert_eq!(h.resources().get(&30).unwrap().len(), 2);
        assert_eq!(h.resources().get(&5), Some(&vec![(2, 2)]));
        assert_eq!(
            h.resources().get(&18),
            Some(&vec![(7, 7), (10, 10), (16, 16)])
        );
        assert_eq!(
            h.resources().get(&27),
            Some(&vec![(21, 21), (23, 23), (24, 24)])
        );
        assert_eq!(h.resources().get(&30), Some(&vec![(28, 28), (29, 29)]));
    }
}
