use hash_bench::hash_ring::{HashRing, HashRingInterface};

fn main() {
    let mut h = HashRing::new(5);
    h.add_node(3);
    h.add_node(5);
    h.add_node(10);
    h.add_node(11);
    h.print();
}
