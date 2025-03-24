use log::debug;
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hasher},
};

type HashBytes = Vec<u8>;
type Key = u64;

pub trait Node: std::fmt::Debug {
    fn name(&self) -> HashBytes;
}

pub struct HashRing<N: Node, H = DefaultHasher> {
    hasher: H,
    nodes: BTreeMap<Key, N>,
}

impl<N: Node> Default for HashRing<N> {
    fn default() -> Self {
        HashRing {
            hasher: DefaultHasher::new(),
            nodes: BTreeMap::new(),
        }
    }
}

impl<N: Node> HashRing<N> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<N: Node, H: Hasher> HashRing<N, H> {
    pub fn add_nodes(&mut self, nodes: Vec<N>) {
        for node in nodes {
            self.add_node(node);
        }
    }
    pub fn remove_nodes(&mut self, nodes: Vec<N>) {
        for node in nodes {
            self.remove_node(node);
        }
    }
    pub fn lookup(&mut self, id: &HashBytes) -> Option<&N> {
        if self.nodes.is_empty() {
            return None;
        }

        let key = _get_key::<H>(&mut self.hasher, &id);
        let node = self.nodes.range(key..).next();
        if let Some((_key, _value)) = node {
            debug!("Node found: [{}] {:?}", key, node);
            return Some(_value);
        }

        // if we reach here, we wrap around the ring
        let first = self.nodes.iter().next();
        if let Some((_key, _value)) = first {
            debug!("Node found: [{}] {:?}", key, first);
            return Some(_value);
        }
        None
    }

    fn add_node(&mut self, node: N) {
        let name = node.name();
        let id = _get_key::<H>(&mut self.hasher, &name);
        debug!(
            "Node added: [{}] len = {} | {:?}",
            id,
            self.nodes.len(),
            node
        );
        self.nodes.insert(id, node);
    }
    fn remove_node(&mut self, node: N) {
        let name = node.name();
        let id = _get_key::<H>(&mut self.hasher, &name);
        let _node = self.nodes.remove(&id);
        debug!(
            "Node removed: [{}] len = {} | {:?}",
            id,
            self.nodes.len(),
            _node
        );
    }
}

// an internal function for looking up the key of the node
fn _get_key<H: Hasher>(hasher: &mut H, data: &HashBytes) -> Key {
    hasher.write(&data);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use crate::log::init_test_logger;

    use super::*;

    #[derive(Debug)]
    struct TestNode {
        name: String,
    }

    impl TestNode {
        fn new(name: &str) -> Self {
            TestNode {
                name: name.to_string(),
            }
        }
    }

    static F: fn(&String) -> HashBytes = |x: &String| -> HashBytes { x.as_bytes().to_vec() };
    impl Node for TestNode {
        fn name(&self) -> HashBytes {
            F(&self.name)
        }
    }

    #[test]
    fn check_hash() {
        init_test_logger();

        let node = TestNode::new("hoge");
        assert_eq!(node.name(), F(&"hoge".to_string()));
    }

    #[test]
    fn add_and_remove_nodes() {
        init_test_logger();

        let mut ring = HashRing::new();
        let hoge = TestNode::new("hoge");
        let fuga = TestNode::new("fuga");
        let piyo = TestNode::new("piyo");

        ring.add_nodes(vec![hoge, fuga]);
        assert_eq!(2, ring.nodes.len());
        let found = ring.lookup(&F(&"hoge".to_string()));
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), F(&"hoge".to_string()));
    }
}
