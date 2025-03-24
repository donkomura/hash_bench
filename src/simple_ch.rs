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

        let key = _get_key::<H>(&mut self.hasher, id.clone());
        let node = self.nodes.range(key..).next();
        if let Some((_key, _value)) = node {
            return Some(_value);
        }

        let first = self.nodes.iter().next();
        let (_key, _value) = first.unwrap();
        Some(_value)
    }

    fn add_node(&mut self, node: N) {
        let name = node.name();
        let id = _get_key::<H>(&mut self.hasher, name);
        self.nodes.insert(id, node.clone());
        debug!(
            "Node added: [{}] len = {} | {:?}",
            id,
            self.nodes.len(),
            node
        );
    }
    fn remove_node(&mut self, node: N) {
        let name = node.name();
        let id = _get_key::<H>(&mut self.hasher, name);
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
fn _get_key<H: Hasher>(hasher: &mut H, data: HashBytes) -> Key {
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

    impl Node for TestNode {
        fn name(&self) -> HashBytes {
            self.name.as_bytes().to_vec()
        }
    }

    #[test]
    fn check_hasher() {
        init_test_logger();
        let mut hasher = DefaultHasher::new();
        let data = "hello".as_bytes();
        assert_eq!(
            16350172494705860510u64,
            _get_key(&mut hasher, data.to_vec()),
        )
    }

    #[test]
    fn add_and_remove_nodes() {
        init_test_logger();
        let mut ring = HashRing::new();
        let node1 = TestNode::new("node1");
        let node2 = TestNode::new("node2");
        let node3 = TestNode::new("node3");
        let node4 = TestNode::new("node4");
        let node5 = TestNode::new("node5");
    }
}
