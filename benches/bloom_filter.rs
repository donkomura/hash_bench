use criterion::{criterion_group, criterion_main, Criterion};

use hash_bench::bloom_filter::BloomFilter;

fn bench_bloom_filter(c: &mut Criterion) {
    c.bench_function("bench_bloom_filter", |b| {
        b.iter(|| {
            let f = 0.01;
            let n = 100;
            for i in 1..=n {
                let mut b = BloomFilter::new(n, f);
                b.insert(&i.to_be_bytes());
            }
            std::hint::black_box(());
        });
    });
}

criterion_group!(benches, bench_bloom_filter,);
criterion_main!(benches);
