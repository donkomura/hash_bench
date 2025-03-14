use criterion::{criterion_group, criterion_main, Criterion};

use hash_bench::BloomFilter;

fn bench_bloom_filter(c: &mut Criterion) {
    c.bench_function("bench_bloom_filter", |b| {
        b.iter(|| {
            let f = 0.01;
            let n = 100;
            std::hint::black_box(for i in 1..=n {
                let mut b = BloomFilter::new(n, f);
                b.insert(&i.to_be_bytes());
            });
        });
    });
}

criterion_group!(benches,
    bench_bloom_filter,
);
criterion_main!(benches);
