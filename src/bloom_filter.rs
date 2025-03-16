use bitvec::prelude::BitVec;
use murmurhash3::murmurhash3_x86_32 as mmh3;

pub struct BloomFilter {
    n: u32,
    m: u32,
    k: u32,
    f: f32,
    bit_array: bitvec::prelude::BitVec,
}

impl BloomFilter {
    pub fn new(n: u32, f: f32) -> Self {
        let m = Self::calc_m(n, f);
        let k = Self::calc_k(m, n);
        let mut vec = BitVec::new();
        vec.resize(m.try_into().unwrap(), false);
        BloomFilter {
            n: n,
            m: m,
            k: k,
            f: f,
            bit_array: vec,
        }
    }

    fn calc_m(n: u32, f: f32) -> u32 {
        let x = 2.0f32;
        return (-f.ln() * (n as f32) / x.ln().powi(2)) as u32;
    }
    fn calc_k(m: u32, n: u32) -> u32 {
        let x = 2.0f32;
        return ((m as f32) * x.ln() / (n as f32)) as u32;
    }
    pub fn insert(&mut self, item: &[u8]) {
        for i in 0..self.k {
            let index = mmh3(&item, i) % self.m;
            self.bit_array.set(index as usize, true);
        }
    }
    pub fn lookup(&mut self, item: &[u8]) -> bool {
        for i in 0..self.k {
            let index = mmh3(&item, i) % self.m;
            if self.bit_array[index as usize] == false {
                return false;
            }
        }
        return true;
    }
    pub fn print(self) {
        println!(
            "parameters: n = {}, m = {}, k = {}, f = {}",
            self.n, self.m, self.k, self.f
        );
        print!("bit_array = [ ");
        for v in self.bit_array.as_bitslice() {
            print!("{} ", v);
        }
        println!("]");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn calc_parameters() {
        let b = BloomFilter::new(10, 0.01);
        assert_eq!(b.n, 10);
        assert_eq!(b.f, 0.01);
        assert_eq!(b.m, 95);
        assert_eq!(b.k, 6);
    }
    #[test]
    fn insert_lookup_must_found() {
        let mut b = BloomFilter::new(10, 0.01);
        b.insert(b"1");
        assert_eq!(b.lookup(b"1"), true);
        assert_eq!(b.lookup(b"2"), false);
        b.insert(b"123");
        assert_eq!(b.lookup(b"123"), true);
    }
}
