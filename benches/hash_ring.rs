use criterion::{criterion_group, criterion_main, Criterion};

use hash_bench::hash_ring::{HashRing, HashRingInterface};

fn bench_hash_ring(c: &mut Criterion) {
    c.bench_function("bench_hash_ring", |b| {
        b.iter(|| {
            let k = 5;
            let n = (2 as i32).pow(k);
            for i in 1..n {
                let mut h = HashRing::new(k);
                h.add_node(i);
            }
            std::hint::black_box(());
        });
    });
}

criterion_group!(benches, bench_hash_ring,);
criterion_main!(benches);
