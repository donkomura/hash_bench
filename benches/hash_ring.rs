use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;

use hash_bench::hash_ring::{HashRing, HashRingInterface};

fn bench_hash_ring_resource_adding(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_ring_resource_adding");

    for k in 2u32..=16 {
        group.bench_with_input(BenchmarkId::from_parameter(k), &k, |b, &k| {
            let mut h = HashRing::new(k);
            let n = (2 as i32).pow(k); // means 2^k
            h.add_node(1);
            b.iter(|| {
                let mut rng = rand::rng();
                std::hint::black_box(
                    // adding a new node 2 means that
                    // almost of all resource (2^k-1) in the cluster will be moved to the new node 2
                    h.add_resource(rng.random_range(0..n)),
                );
            });
            h.remove_node(1);
        });
    }
    group.finish();
}

criterion_group!(benches, bench_hash_ring_resource_adding,);
criterion_main!(benches);
