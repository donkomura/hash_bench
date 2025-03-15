use hash_bench::hash_ring::{HashRing, HashRingInterface};

fn main() {
    let mut h = HashRing::new(5);
    h.insert(3);
    h.insert(5);
    let node_ref = h.lookup(3).unwrap();
    println!("node_ref: {:?}", node_ref);
}
