use hash_bench::hash_ring::{HashRing, HashRingInterface};

fn main() {
    let mut h = HashRing::new(5);
    h.insert(3);
    h.insert(5);
    h.insert(10);
    h.insert(11);
    h.print();
}
