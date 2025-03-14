use hash_bench::bloom_filter::bloom_filter::BloomFilter;

fn main() {
    let mut b = BloomFilter::new(10, 0.01);
    b.insert(b"1");
    b.insert(b"11");
    b.insert(b"32");
    println!("1: {}", b.lookup(b"1"));
    println!("8: {}", b.lookup(b"8"));
    println!("11: {}", b.lookup(b"11"));
    println!("44: {}", b.lookup(b"44"));
    b.print();
}
