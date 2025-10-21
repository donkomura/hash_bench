#[derive(Default, Clone)]
struct Slot {
    remainder: u64,
    is_occupied: bool,
    is_continued: bool,
    is_shifted: bool,
}

impl Slot {
    fn is_empty(&self) -> bool {
        !self.is_occupied && !self.is_continued && !self.is_shifted && self.remainder == 0
    }
}

pub struct QuotientFilter {
    q: u64,
    r: u64,
    entries: usize,
    size: usize,
    filter: Vec<Slot>,
}

impl QuotientFilter {
    pub fn new(q: u64, r: u64) -> Self {
        let size: usize = 1 << q;
        QuotientFilter {
            q,
            r,
            size,
            entries: 0,
            filter: vec![Slot::default(); size],
        }
    }

    pub fn insert(&mut self, key: u64) {
        let (quotient, remainder) = self.split(key);
        let q_idx = quotient as usize;

        // if the slot is empty, insert directly
        if self.filter[q_idx].is_empty() {
            self.filter[q_idx].remainder = remainder;
            self.filter[q_idx].is_occupied = true;
            self.entries += 1;
            return;
        }

        let already_occupied = self.filter[q_idx].is_occupied;
        self.filter[q_idx].is_occupied = true;

        let mut b = q_idx;
        // find the start of the cluster
        while self.filter[b].is_shifted {
            b = (b + self.size - 1) % self.size;
        }
        let mut s = b;
        // find the same quotient run
        while b != q_idx {
            s = (s + 1) % self.size;
            // track the run for this quotient
            while self.filter[s].is_continued {
                s = (s + 1) % self.size;
            }
            b = (b + 1) % self.size;
            // skip empty slots
            while !self.filter[b].is_occupied {
                b = (b + 1) % self.size;
            }
        }

        // remember the start position of the run
        let run_start = s;

        // find the insertion point
        while !self.filter[s].is_empty() && self.filter[s].remainder < remainder {
            s = (s + 1) % self.size;
        }

        // determine if we're inserting at the start of the run
        let is_run_start = s == run_start;

        if self.filter[s].is_empty() {
            self.filter[s].remainder = remainder;
            self.filter[s].is_shifted = s != q_idx;
            self.filter[s].is_continued = already_occupied && !is_run_start;
            self.entries += 1;
            return;
        }

        // shift entries to make space
        // first, find an empty slot
        let mut empty_pos = s;
        while !self.filter[empty_pos].is_empty() {
            empty_pos = (empty_pos + 1) % self.size;
        }

        // shift entries backward from the empty slot
        let mut curr = empty_pos;
        while curr != s {
            let prev = (curr + self.size - 1) % self.size;
            self.filter[curr] = self.filter[prev].clone();
            self.filter[curr].is_shifted = true;
            curr = prev;
        }

        // set the new remainder at the insertion position
        self.filter[s].remainder = remainder;
        self.filter[s].is_shifted = s != q_idx;
        self.filter[s].is_continued = !is_run_start;

        // if inserting at the start of the run, set is_continued=true for the next slot (shifted original run start)
        if is_run_start {
            let next = (s + 1) % self.size;
            self.filter[next].is_continued = true;
        }

        self.entries += 1;
    }

    fn split(&self, key: u64) -> (u64, u64) {
        let quotient = (key >> self.r) & ((1 << self.q) - 1);
        let remainder = key & ((1 << self.r) - 1);
        (quotient, remainder)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_split() {
        let qf = QuotientFilter::new(8, 4);
        let (quotient, remainder) = qf.split(0b111111110000);
        assert_eq!(quotient, 0b11111111);
        assert_eq!(remainder, 0b0000);
    }

    #[test]
    fn test_insert_empty_slot() {
        // Case 1: Insert into an empty slot
        let mut qf = QuotientFilter::new(4, 4);
        let key = 0b00010001; // quotient=0b0001, remainder=0b0001
        qf.insert(key);

        assert_eq!(qf.entries, 1);

        let (quotient, remainder) = qf.split(key);
        let idx = quotient as usize;
        assert_eq!(qf.filter[idx].remainder, remainder);
        assert!(qf.filter[idx].is_occupied);
        assert!(!qf.filter[idx].is_continued);
        assert!(!qf.filter[idx].is_shifted);
    }

    #[test]
    fn test_insert_same_quotient_different_remainder() {
        // Case 2: Insert when slot is already occupied (same quotient, different remainder)
        let mut qf = QuotientFilter::new(4, 4);

        // insert the first key (quotient=0b0001, remainder=0b0001)
        let key1 = 0b00010001;
        qf.insert(key1);

        // insert a key with the same quotient but different remainder (quotient=0b0001, remainder=0b0010)
        let key2 = 0b00010010;
        qf.insert(key2);

        assert_eq!(qf.entries, 2);

        let (quotient, _) = qf.split(key1);
        let idx = quotient as usize;
        assert!(qf.filter[idx].is_occupied);

        // the first remainder is stored in the quotient slot
        assert_eq!(qf.filter[idx].remainder, 0b0001);
        assert!(!qf.filter[idx].is_continued);

        // the second remainder is stored in the next slot with continued flag set
        assert_eq!(qf.filter[idx + 1].remainder, 0b0010);
        assert!(qf.filter[idx + 1].is_continued);
        assert!(qf.filter[idx + 1].is_shifted);
    }

    #[test]
    fn test_insert_with_shifting() {
        // Case 3: Insert when slots are occupied and need to shift remainder positions
        let mut qf = QuotientFilter::new(4, 4);

        let key1 = 0b00010010;
        qf.insert(key1);

        let key2 = 0b00010011;
        qf.insert(key2);

        // this should be inserted between key1 and key2 (sorted by remainder)
        let key3 = 0b00010001;
        qf.insert(key3);

        assert_eq!(qf.entries, 3);

        let idx = 1;
        assert!(qf.filter[idx].is_occupied);

        assert_eq!(qf.filter[idx].remainder, 0b0001);
        assert_eq!(qf.filter[idx + 1].remainder, 0b0010);
        assert_eq!(qf.filter[idx + 2].remainder, 0b0011);

        // the first element should have continued = false
        assert!(!qf.filter[idx].is_continued);
        assert!(qf.filter[idx + 1].is_continued);
        assert!(qf.filter[idx + 2].is_continued);
    }

    #[test]
    fn test_insert_different_quotients_collision() {
        // Case 4: Collision with keys having different quotients (cluster formation)
        let mut qf = QuotientFilter::new(4, 4);

        let key1 = 0b00010001;
        qf.insert(key1);
        let key2 = 0b00100010;
        qf.insert(key2);
        let key3 = 0b00010011;
        qf.insert(key3);

        assert_eq!(qf.entries, 3);

        // quotient=0b0001 slot (first remainder)
        assert!(qf.filter[1].is_occupied);
        assert_eq!(qf.filter[1].remainder, 0b0001);
        assert!(!qf.filter[1].is_shifted);
        assert!(!qf.filter[1].is_continued);

        // quotient=0b0010 slot
        assert!(qf.filter[2].is_occupied);
        assert_eq!(qf.filter[2].remainder, 0b0010);
        assert!(!qf.filter[2].is_shifted);
        assert!(!qf.filter[2].is_continued);

        // second remainder for quotient=0b0001 (shifted)
        assert_eq!(qf.filter[3].remainder, 0b0011);
        assert!(qf.filter[3].is_shifted);
        assert!(qf.filter[3].is_continued);
    }

    #[test]
    fn test_insert_duplicate_key() {
        // Case 5: Insert duplicate keys
        let mut qf = QuotientFilter::new(4, 4);

        let key = 0b00010001;
        qf.insert(key);
        qf.insert(key); // insert the same key again

        // for duplicate keys, entry count becomes 2 (Quotient Filter allows duplicates)
        assert_eq!(qf.entries, 2);

        let idx = 1;
        assert_eq!(qf.filter[idx].remainder, 0b0001);
        assert_eq!(qf.filter[idx + 1].remainder, 0b0001);
    }

    #[test]
    fn test_insert_wraparound() {
        // Case 6: Ring buffer wraparound
        let mut qf = QuotientFilter::new(4, 4); // size 16

        // insert a key with quotient=15 (last slot)
        let key1 = 0b11110001;
        qf.insert(key1);

        // insert another key with quotient=15 (wraparound may occur)
        let key2 = 0b11110010;
        qf.insert(key2);

        assert_eq!(qf.entries, 2);

        let idx = 15;
        assert!(qf.filter[idx].is_occupied);
        assert_eq!(qf.filter[idx].remainder, 0b0001);

        // next slot wraps around to 0
        assert_eq!(qf.filter[0].remainder, 0b0010);
        assert!(qf.filter[0].is_shifted);
        assert!(qf.filter[0].is_continued);
    }

    #[test]
    fn test_insert_multiple_runs_with_shift_and_order() {
        let mut qf = QuotientFilter::new(4, 4);

        // quotient=1 run (ascending order)
        qf.insert(0b0001_0001);
        qf.insert(0b0001_0010);

        // quotient=2 run (insert in reverse order to test sorting)
        qf.insert(0b0010_0011);
        qf.insert(0b0010_0001);

        // quotient=3 run (single element)
        qf.insert(0b0011_0010);

        assert_eq!(qf.entries, 5);

        assert!(
            qf.filter[1].is_occupied,
            "q=1 should set occupied at bucket 1"
        );
        assert!(
            qf.filter[2].is_occupied,
            "q=2 should set occupied at bucket 2"
        );
        assert!(
            qf.filter[3].is_occupied,
            "q=3 should set occupied at bucket 3"
        );

        assert_eq!(qf.filter[1].remainder, 0b0001);
        assert!(!qf.filter[1].is_continued);
        assert!(!qf.filter[1].is_shifted, "first of q=1 is at home");

        assert_eq!(qf.filter[2].remainder, 0b0010);
        assert!(qf.filter[2].is_continued);
        assert!(
            qf.filter[2].is_shifted,
            "q=1 second element must be shifted"
        );

        // q=2 run: index=3,4 → remainders [1,3] (verify ascending order)
        assert_eq!(
            qf.filter[3].remainder, 0b0001,
            "q=2 run must be sorted: 1 then 3"
        );
        assert!(!qf.filter[3].is_continued);
        assert!(
            qf.filter[3].is_shifted,
            "q=2 first element is not at home (home=2)"
        );

        assert_eq!(qf.filter[4].remainder, 0b0011);
        assert!(qf.filter[4].is_continued);
        assert!(qf.filter[4].is_shifted);

        // q=3 run: index=5 → remainder [2]
        assert_eq!(qf.filter[5].remainder, 0b0010);
        assert!(!qf.filter[5].is_continued);
        assert!(
            qf.filter[5].is_shifted,
            "q=3 first element is not at home (home=3)"
        );

        // ---- additional sanity checks (run boundaries and ordering) ----
        // 1) run heads must have is_continued=0
        for &i in &[1, 3, 5] {
            assert!(
                !qf.filter[i].is_continued,
                "run head must have is_continued=0 at {}",
                i
            );
        }
        // 2) run bodies (non-heads) must have is_continued=1
        for &i in &[2, 4] {
            assert!(
                qf.filter[i].is_continued,
                "run body must have is_continued=1 at {}",
                i
            );
        }
        // 3) q=2's home (index=2) has occupied=1, but storage position is at 3 or later (= shifted elements exist)
        assert!(qf.filter[2].is_occupied);
        assert_ne!(
            qf.filter[2].remainder, 0b0001,
            "index=2 should not store q=2's first element"
        );
    }
}
