use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};

use hash_bench::quotient_filter::QuotientFilter;

fn bench_quotient_filter_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("quotient_filter_insert");
    let r = 8;
    let load_factors = [25usize, 50, 75];
    let qs = [10u64, 12u64];

    for &q in &qs {
        let capacity = 1usize << q;
        for &load in &load_factors {
            let target_entries = capacity * load / 100;
            let mut rng = StdRng::seed_from_u64(0xC0FFEEu64 ^ ((q as u64) << 32) ^ load as u64);
            let keys: Vec<u64> = (0..target_entries).map(|_| rng.random()).collect();
            let bench_id = BenchmarkId::new(format!("q{q}"), format!("{load}pct"));

            group.bench_with_input(bench_id, &target_entries, |b, &_entries| {
                b.iter_batched(
                    || QuotientFilter::new(q, r),
                    |mut filter| {
                        for &key in &keys {
                            filter.insert(key);
                        }
                        filter
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_quotient_filter_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("quotient_filter_lookup");
    let r = 8;
    let qs = [10u64, 12u64];
    let probe_ratio = 10; // number of lookups relative to inserted keys

    for &q in &qs {
        let capacity = 1usize << q;
        let target_entries = capacity / 2;
        let mut rng = StdRng::seed_from_u64(0xFACEFEEDu64 ^ ((q as u64) << 32));
        let keys: Vec<u64> = (0..target_entries).map(|_| rng.random()).collect();
        let probes: Vec<u64> = (0..target_entries * probe_ratio)
            .map(|i| {
                if i % probe_ratio == 0 {
                    keys[i / probe_ratio]
                } else {
                    rng.random()
                }
            })
            .collect();
        let bench_id = BenchmarkId::from_parameter(format!("q{q}"));

        group.bench_with_input(bench_id, &target_entries, |b, &_entries| {
            b.iter_batched(
                || {
                    let mut filter = QuotientFilter::new(q, r);
                    for &key in &keys {
                        filter.insert(key);
                    }
                    filter
                },
                |filter| {
                    for &probe in &probes {
                        std::hint::black_box(filter.lookup(probe));
                    }
                    filter
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_quotient_filter_insert,
    bench_quotient_filter_lookup
);
criterion_main!(benches);
