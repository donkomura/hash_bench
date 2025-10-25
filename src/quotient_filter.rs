#[derive(Clone, Default)]
struct Slot {
    data: u64,
}

const FLAG_BITS: u64 = 3;
const FLAG_MASK: u64 = (1 << FLAG_BITS) - 1;
const FLAG_OCCUPIED: u64 = 1 << 0;
const FLAG_CONTINUED: u64 = 1 << 1;
const FLAG_SHIFTED: u64 = 1 << 2;

impl Slot {
    fn is_empty(&self) -> bool {
        self.data == 0
    }

    fn remainder(&self) -> u64 {
        self.data >> FLAG_BITS
    }

    fn set_remainder(&mut self, remainder: u64) {
        let flags = self.data & FLAG_MASK;
        self.data = (remainder << FLAG_BITS) | flags;
    }

    fn is_occupied(&self) -> bool {
        (self.data & FLAG_OCCUPIED) != 0
    }

    fn set_occupied(&mut self, value: bool) {
        if value {
            self.data |= FLAG_OCCUPIED;
        } else {
            self.data &= !FLAG_OCCUPIED;
        }
    }

    fn is_continued(&self) -> bool {
        (self.data & FLAG_CONTINUED) != 0
    }

    fn set_continued(&mut self, value: bool) {
        if value {
            self.data |= FLAG_CONTINUED;
        } else {
            self.data &= !FLAG_CONTINUED;
        }
    }

    fn is_shifted(&self) -> bool {
        (self.data & FLAG_SHIFTED) != 0
    }

    fn set_shifted(&mut self, value: bool) {
        if value {
            self.data |= FLAG_SHIFTED;
        } else {
            self.data &= !FLAG_SHIFTED;
        }
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

    fn prev_index(&self, idx: usize) -> usize {
        (idx + self.size - 1) % self.size
    }

    fn next_index(&self, idx: usize) -> usize {
        (idx + 1) % self.size
    }

    fn find_run_head(&self, home_idx: usize) -> usize {
        let mut bucket = home_idx;
        while self.filter[bucket].is_shifted() {
            bucket = self.prev_index(bucket);
        }

        let mut run_head = bucket;
        let mut probe = bucket;
        while probe != home_idx {
            run_head = self.next_index(run_head);
            while self.filter[run_head].is_continued() {
                run_head = self.next_index(run_head);
            }
            probe = self.next_index(probe);
            while !self.filter[probe].is_occupied() {
                probe = self.next_index(probe);
            }
        }
        run_head
    }

    /// Run内の全ての要素に対してクロージャを実行する
    ///
    /// * `run_head`: runの先頭スロットのインデックス
    /// * `f`: 各スロットインデックスに対して実行されるクロージャ
    fn visit_run<F>(&self, run_head: usize, mut f: F)
    where
        F: FnMut(usize),
    {
        f(run_head);
        let mut idx = self.next_index(run_head);
        while self.filter[idx].is_continued() {
            f(idx);
            idx = self.next_index(idx);
        }
    }

    fn collect_keys(&self) -> Vec<u64> {
        let mut keys = Vec::with_capacity(self.entries);
        if self.entries == 0 {
            return keys;
        }

        let size = self.size;
        for quotient_idx in 0..size {
            if !self.filter[quotient_idx].is_occupied() {
                continue;
            }

            let run_head = self.find_run_head(quotient_idx);
            self.visit_run(run_head, |slot_idx| {
                let key = ((quotient_idx as u64) << self.r) | self.filter[slot_idx].remainder();
                keys.push(key);
            });
        }

        keys
    }

    pub fn resize(&mut self) {
        let new_q = self.q + 1;

        let keys = self.collect_keys();
        let mut new_qf = QuotientFilter::new(new_q, self.r);
        for key in keys {
            new_qf.insert(key);
        }

        *self = new_qf;
    }

    pub fn merge(&self, other: &Self) -> Self {
        assert_eq!(
            self.r, other.r,
            "cannot merge filters with different remainder sizes"
        );

        let keys_self = self.collect_keys();
        let keys_other = other.collect_keys();
        let total_entries = keys_self.len() + keys_other.len();

        let mut target_q = self.q.max(other.q);
        let mut capacity = (1usize)
            .checked_shl(target_q as u32)
            .expect("q too large for usize");
        while capacity < total_entries {
            target_q += 1;
            capacity = (1usize)
                .checked_shl(target_q as u32)
                .expect("q too large for usize");
        }

        let mut merged = QuotientFilter::new(target_q, self.r);
        for key in keys_self.into_iter().chain(keys_other.into_iter()) {
            merged.insert(key);
        }

        merged
    }

    pub fn insert(&mut self, key: u64) {
        if self.entries == self.size {
            self.resize();
        }

        let (quotient, remainder) = self.split(key);
        let q_idx = quotient as usize;

        // if the slot is empty, insert directly
        if self.filter[q_idx].is_empty() {
            self.filter[q_idx].set_remainder(remainder);
            self.filter[q_idx].set_occupied(true);
            self.entries += 1;
            return;
        }

        let already_occupied = self.filter[q_idx].is_occupied();
        self.filter[q_idx].set_occupied(true);

        let run_head = self.find_run_head(q_idx);
        let mut insert_pos = run_head;
        if !self.filter[insert_pos].is_empty() && self.filter[insert_pos].remainder() < remainder {
            loop {
                insert_pos = self.next_index(insert_pos);
                if !(self.filter[insert_pos].is_continued()
                    && self.filter[insert_pos].remainder() < remainder)
                {
                    break;
                }
            }
        }

        let inserting_at_head = insert_pos == run_head;

        if self.filter[insert_pos].is_empty() {
            self.filter[insert_pos].set_remainder(remainder);
            self.filter[insert_pos].set_shifted(insert_pos != q_idx);
            self.filter[insert_pos].set_continued(already_occupied && !inserting_at_head);
            self.entries += 1;
            return;
        }

        // shift entries to make space
        let mut empty_pos = insert_pos;
        while !self.filter[empty_pos].is_empty() {
            empty_pos = self.next_index(empty_pos);
        }

        // shift entries backward from the empty slot
        let mut curr = empty_pos;
        while curr != insert_pos {
            let prev = self.prev_index(curr);
            let prev_slot = self.filter[prev].clone();
            self.filter[curr].set_remainder(prev_slot.remainder());
            self.filter[curr].set_continued(prev_slot.is_continued());
            self.filter[curr].set_shifted(true);
            curr = prev;
        }

        // set the new remainder at the insertion position
        self.filter[insert_pos].set_remainder(remainder);
        self.filter[insert_pos].set_shifted(insert_pos != q_idx);
        self.filter[insert_pos].set_continued(already_occupied && !inserting_at_head);

        // if inserting at the start of the run, set is_continued=true for the next slot (shifted original run start)
        if inserting_at_head {
            let next = self.next_index(insert_pos);
            self.filter[next].set_continued(true);
        }

        self.entries += 1;
    }

    pub fn lookup(&self, key: u64) -> bool {
        let (quotient, remainder) = self.split(key);
        let q_idx = quotient as usize;
        if !self.filter[q_idx].is_occupied() {
            return false;
        }

        let run_head = self.find_run_head(q_idx);
        if self.filter[run_head].remainder() == remainder {
            return true;
        }

        let mut idx = self.next_index(run_head);
        while self.filter[idx].is_continued() {
            if self.filter[idx].remainder() == remainder {
                return true;
            }
            idx = self.next_index(idx);
        }

        // Reached end of run (next run start or empty slot)
        false
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
        assert_eq!(qf.filter[idx].remainder(), remainder);
        assert!(qf.filter[idx].is_occupied());
        assert!(!qf.filter[idx].is_continued());
        assert!(!qf.filter[idx].is_shifted());
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
        assert!(qf.filter[idx].is_occupied());

        // the first remainder is stored in the quotient slot
        assert_eq!(qf.filter[idx].remainder(), 0b0001);
        assert!(!qf.filter[idx].is_continued());

        // the second remainder is stored in the next slot with continued flag set
        assert_eq!(qf.filter[idx + 1].remainder(), 0b0010);
        assert!(qf.filter[idx + 1].is_continued());
        assert!(qf.filter[idx + 1].is_shifted());
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
        assert!(qf.filter[idx].is_occupied());

        assert_eq!(qf.filter[idx].remainder(), 0b0001);
        assert_eq!(qf.filter[idx + 1].remainder(), 0b0010);
        assert_eq!(qf.filter[idx + 2].remainder(), 0b0011);

        // the first element should have continued = false
        assert!(!qf.filter[idx].is_continued());
        assert!(qf.filter[idx + 1].is_continued());
        assert!(qf.filter[idx + 2].is_continued());
    }

    #[test]
    fn test_insert_preserves_occupied_bitmap() {
        let mut qf = QuotientFilter::new(4, 4);

        // Insert larger remainder first so the later insert shifts the run head.
        qf.insert(0b0001_0010);
        qf.insert(0b0001_0001);

        assert!(
            qf.filter[1].is_occupied(),
            "home bucket for quotient=1 must remain occupied"
        );
        assert!(
            !qf.filter[2].is_occupied(),
            "inserting only quotient=1 elements must not mark quotient=2 as occupied"
        );
    }

    #[test]
    fn test_resize_expands_capacity() {
        let mut qf = QuotientFilter::new(3, 4); // size = 8

        let initial_keys: Vec<u64> = (0..8).map(|q| (q << qf.r) | 0b0001).collect();
        for key in &initial_keys {
            qf.insert(*key);
        }
        assert_eq!(qf.entries, 8);
        assert_eq!(qf.size, 8);

        qf.resize();

        assert_eq!(qf.size, 16);
        assert_eq!(qf.q, 4);
        for key in &initial_keys {
            assert!(qf.lookup(*key), "key {:x} should survive resize", key);
        }

        let additional_keys: Vec<u64> = (8..16).map(|q| (q << qf.r) | 0b0010).collect();
        for key in &additional_keys {
            qf.insert(*key);
        }

        assert_eq!(qf.entries, 16);
        for key in initial_keys.iter().chain(additional_keys.iter()) {
            assert!(
                qf.lookup(*key),
                "key {:x} should be present after resize and additional inserts",
                key
            );
        }
    }

    #[test]
    fn test_merge_combines_filters() {
        let mut left = QuotientFilter::new(4, 4);
        let mut right = QuotientFilter::new(4, 4);

        let left_keys = vec![0b0001_0001, 0b0010_0010, 0b0011_0011];
        let right_keys = vec![0b0100_0001, 0b0101_0010];

        for key in &left_keys {
            left.insert(*key);
        }
        for key in &right_keys {
            right.insert(*key);
        }

        let merged = left.merge(&right);

        assert_eq!(merged.entries, left.entries + right.entries);

        for key in left_keys.iter().chain(right_keys.iter()) {
            assert!(
                merged.lookup(*key),
                "merged filter should contain {:08b}",
                key
            );
        }

        for key in &left_keys {
            assert!(left.lookup(*key), "left filter must remain unchanged");
        }
        for key in &right_keys {
            assert!(right.lookup(*key), "right filter must remain unchanged");
        }
    }

    #[test]
    fn test_merge_resizes_and_preserves_duplicates() {
        let mut left = QuotientFilter::new(3, 4);
        let mut right = QuotientFilter::new(3, 4);

        let left_keys: Vec<u64> = (0..8).map(|q| (q << left.r) | 0b0001).collect();
        for key in &left_keys {
            left.insert(*key);
        }
        left.insert(left_keys[0]); // duplicate

        let right_keys: Vec<u64> = (0..8).map(|q| ((q as u64) << right.r) | 0b0010).collect();
        for key in &right_keys {
            right.insert(*key);
        }
        right.insert(right_keys[0]); // duplicate

        let merged = left.merge(&right);

        assert_eq!(left.entries, left_keys.len() + 1);
        assert_eq!(right.entries, right_keys.len() + 1);

        assert_eq!(
            merged.entries,
            left.entries + right.entries,
            "merged entries should account for duplicates"
        );
        assert!(
            merged.size >= left.size && merged.size >= right.size,
            "merged filter should be at least as large as inputs"
        );

        for key in left_keys.iter().chain(right_keys.iter()) {
            assert!(
                merged.lookup(*key),
                "merged filter should contain {:08b}",
                key
            );
        }
        assert!(
            merged.lookup(left_keys[0]),
            "duplicate key must be present in merged filter"
        );
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
        assert!(qf.filter[1].is_occupied());
        assert_eq!(qf.filter[1].remainder(), 0b0001);
        assert!(!qf.filter[1].is_shifted());
        assert!(!qf.filter[1].is_continued());

        // quotient=0b0010 slot
        assert!(qf.filter[2].is_occupied());

        // With the corrected insert, quotient=1's run should be contiguous
        // so filter[2] should contain the second element of quotient=1's run
        assert_eq!(qf.filter[2].remainder(), 0b0011);
        assert!(qf.filter[2].is_shifted());
        assert!(qf.filter[2].is_continued());

        // quotient=0b0010's element is shifted to filter[3]
        assert_eq!(qf.filter[3].remainder(), 0b0010);
        assert!(qf.filter[3].is_shifted());
        assert!(!qf.filter[3].is_continued());
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
        assert_eq!(qf.filter[idx].remainder(), 0b0001);
        assert_eq!(qf.filter[idx + 1].remainder(), 0b0001);
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
        assert!(qf.filter[idx].is_occupied());
        assert_eq!(qf.filter[idx].remainder(), 0b0001);

        // next slot wraps around to 0
        assert_eq!(qf.filter[0].remainder(), 0b0010);
        assert!(qf.filter[0].is_shifted());
        assert!(qf.filter[0].is_continued());
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
            qf.filter[1].is_occupied(),
            "q=1 should set occupied at bucket 1"
        );
        assert!(
            qf.filter[2].is_occupied(),
            "q=2 should set occupied at bucket 2"
        );
        assert!(
            qf.filter[3].is_occupied(),
            "q=3 should set occupied at bucket 3"
        );

        assert_eq!(qf.filter[1].remainder(), 0b0001);
        assert!(!qf.filter[1].is_continued());
        assert!(!qf.filter[1].is_shifted(), "first of q=1 is at home");

        assert_eq!(qf.filter[2].remainder(), 0b0010);
        assert!(qf.filter[2].is_continued());
        assert!(
            qf.filter[2].is_shifted(),
            "q=1 second element must be shifted"
        );

        // q=2 run: index=3,4 → remainders [1,3] (verify ascending order)
        assert_eq!(
            qf.filter[3].remainder(),
            0b0001,
            "q=2 run must be sorted: 1 then 3"
        );
        assert!(!qf.filter[3].is_continued());
        assert!(
            qf.filter[3].is_shifted(),
            "q=2 first element is not at home (home=2)"
        );

        assert_eq!(qf.filter[4].remainder(), 0b0011);
        assert!(qf.filter[4].is_continued());
        assert!(qf.filter[4].is_shifted());

        // q=3 run: index=5 → remainder [2]
        assert_eq!(qf.filter[5].remainder(), 0b0010);
        assert!(!qf.filter[5].is_continued());
        assert!(
            qf.filter[5].is_shifted(),
            "q=3 first element is not at home (home=3)"
        );

        // ---- additional sanity checks (run boundaries and ordering) ----
        // 1) run heads must have is_continued=0
        for &i in &[1, 3, 5] {
            assert!(
                !qf.filter[i].is_continued(),
                "run head must have is_continued=0 at {}",
                i
            );
        }
        // 2) run bodies (non-heads) must have is_continued=1
        for &i in &[2, 4] {
            assert!(
                qf.filter[i].is_continued(),
                "run body must have is_continued=1 at {}",
                i
            );
        }
        // 3) q=2's home (index=2) has occupied=1, but storage position is at 3 or later (= shifted elements exist)
        assert!(qf.filter[2].is_occupied());
        assert_ne!(
            qf.filter[2].remainder(),
            0b0001,
            "index=2 should not store q=2's first element"
        );
    }

    #[test]
    fn test_lookup_empty_filter() {
        let qf = QuotientFilter::new(4, 4);
        let key = 0b00010001;
        assert!(!qf.lookup(key));
    }

    #[test]
    fn test_lookup_simple_hit() {
        let mut qf = QuotientFilter::new(4, 4);
        let key = 0b00010001;
        let (quotient, remainder) = qf.split(key);
        let idx = quotient as usize;

        qf.filter[idx].set_remainder(remainder);
        qf.filter[idx].set_occupied(true);
        qf.filter[idx].set_continued(false);
        qf.filter[idx].set_shifted(false);
        qf.entries = 1;

        assert!(qf.lookup(key));
    }

    #[test]
    fn test_lookup_with_run() {
        let mut qf = QuotientFilter::new(4, 4);
        let quotient = 0b0001;
        let idx = quotient as usize;

        qf.filter[idx].set_remainder(0b0001);
        qf.filter[idx].set_occupied(true);
        qf.filter[idx].set_continued(false);
        qf.filter[idx].set_shifted(false);

        qf.filter[idx + 1].set_remainder(0b0010);
        qf.filter[idx + 1].set_occupied(false);
        qf.filter[idx + 1].set_continued(true);
        qf.filter[idx + 1].set_shifted(true);

        qf.filter[idx + 2].set_remainder(0b0011);
        qf.filter[idx + 2].set_occupied(false);
        qf.filter[idx + 2].set_continued(true);
        qf.filter[idx + 2].set_shifted(true);

        qf.entries = 3;

        let key1 = (quotient << qf.r) | 0b0001;
        let key2 = (quotient << qf.r) | 0b0010;
        let key3 = (quotient << qf.r) | 0b0011;
        let key4 = (quotient << qf.r) | 0b0100; // not in the filter

        assert!(qf.lookup(key1));
        assert!(qf.lookup(key2));
        assert!(qf.lookup(key3));
        assert!(!qf.lookup(key4));
    }

    #[test]
    fn test_lookup_multiple_different_quotients() {
        let mut qf = QuotientFilter::new(4, 4);

        qf.filter[1].set_remainder(0b0001);
        qf.filter[1].set_occupied(true);
        qf.filter[1].set_continued(false);
        qf.filter[1].set_shifted(false);

        qf.filter[3].set_remainder(0b0010);
        qf.filter[3].set_occupied(true);
        qf.filter[3].set_continued(false);
        qf.filter[3].set_shifted(false);

        qf.filter[5].set_remainder(0b0011);
        qf.filter[5].set_occupied(true);
        qf.filter[5].set_continued(false);
        qf.filter[5].set_shifted(false);

        qf.filter[7].set_remainder(0b0100);
        qf.filter[7].set_occupied(true);
        qf.filter[7].set_continued(false);
        qf.filter[7].set_shifted(false);

        qf.entries = 4;

        // Test that each different quotient can be found
        let key1 = (0b0001 << qf.r) | 0b0001;
        let key2 = (0b0011 << qf.r) | 0b0010;
        let key3 = (0b0101 << qf.r) | 0b0011;
        let key4 = (0b0111 << qf.r) | 0b0100;

        assert!(qf.lookup(key1), "quotient=1 should be found");
        assert!(qf.lookup(key2), "quotient=3 should be found");
        assert!(qf.lookup(key3), "quotient=5 should be found");
        assert!(qf.lookup(key4), "quotient=7 should be found");

        // Test that non-existent quotients return false
        let key_missing1 = (0b0010 << qf.r) | 0b0001;
        let key_missing2 = (0b0100 << qf.r) | 0b0010;
        let key_missing3 = (0b0110 << qf.r) | 0b0011;

        assert!(!qf.lookup(key_missing1), "quotient=2 should not be found");
        assert!(!qf.lookup(key_missing2), "quotient=4 should not be found");
        assert!(!qf.lookup(key_missing3), "quotient=6 should not be found");

        // Test that same quotient with different remainder returns false
        let key_wrong_remainder1 = (0b0001 << qf.r) | 0b0010;
        let key_wrong_remainder2 = (0b0011 << qf.r) | 0b0001;

        assert!(
            !qf.lookup(key_wrong_remainder1),
            "quotient=1 with wrong remainder should not be found"
        );
        assert!(
            !qf.lookup(key_wrong_remainder2),
            "quotient=3 with wrong remainder should not be found"
        );
    }

    #[test]
    fn test_lookup_with_insert_single() {
        let mut qf = QuotientFilter::new(4, 4);
        let key = 0b00010001;

        qf.insert(key);
        assert!(qf.lookup(key), "inserted key should be found");

        let non_existent = 0b00010010;
        assert!(
            !qf.lookup(non_existent),
            "non-existent key should not be found"
        );
    }

    #[test]
    fn test_lookup_with_insert_multiple_same_quotient() {
        let mut qf = QuotientFilter::new(4, 4);

        let key1 = 0b00010001;
        let key2 = 0b00010010;
        let key3 = 0b00010011;

        qf.insert(key1);
        qf.insert(key2);
        qf.insert(key3);

        assert!(qf.lookup(key1), "key1 should be found");
        assert!(qf.lookup(key2), "key2 should be found");
        assert!(qf.lookup(key3), "key3 should be found");

        let non_existent = 0b00010100;
        assert!(
            !qf.lookup(non_existent),
            "non-existent key should not be found"
        );
    }

    #[test]
    fn test_lookup_with_insert_multiple_different_quotients() {
        let mut qf = QuotientFilter::new(4, 4);

        let key1 = 0b00010001;
        let key2 = 0b00100010;
        let key3 = 0b00110011;
        let key4 = 0b01000100;

        qf.insert(key1);
        qf.insert(key2);
        qf.insert(key3);
        qf.insert(key4);

        assert!(qf.lookup(key1), "key1 should be found");
        assert!(qf.lookup(key2), "key2 should be found");
        assert!(qf.lookup(key3), "key3 should be found");
        assert!(qf.lookup(key4), "key4 should be found");

        let non_existent1 = 0b01010001;
        let non_existent2 = 0b01100010;
        assert!(
            !qf.lookup(non_existent1),
            "non-existent key1 should not be found"
        );
        assert!(
            !qf.lookup(non_existent2),
            "non-existent key2 should not be found"
        );
    }

    #[test]
    fn test_lookup_with_insert_duplicates() {
        let mut qf = QuotientFilter::new(4, 4);
        let key = 0b00010001;

        qf.insert(key);
        qf.insert(key);
        qf.insert(key);

        assert!(qf.lookup(key), "duplicate key should be found");
        assert_eq!(qf.entries, 3, "should have 3 entries for duplicates");
    }

    #[test]
    fn test_lookup_with_insert_collision_scenario() {
        let mut qf = QuotientFilter::new(4, 4);

        let key1 = 0b00010001;
        let key2 = 0b00100010;
        let key3 = 0b00010011;

        qf.insert(key1);
        qf.insert(key2);
        qf.insert(key3);

        assert!(qf.lookup(key1), "key1 should be found after collisions");
        assert!(qf.lookup(key2), "key2 should be found after collisions");
        assert!(qf.lookup(key3), "key3 should be found after collisions");

        let non_existent1 = 0b00010010;
        let non_existent2 = 0b00100001;
        assert!(
            !qf.lookup(non_existent1),
            "non-existent key1 should not be found"
        );
        assert!(
            !qf.lookup(non_existent2),
            "non-existent key2 should not be found"
        );
    }

    #[test]
    fn test_lookup_with_insert_wraparound_scenario() {
        let mut qf = QuotientFilter::new(4, 4);

        let key1 = 0b11110001;
        let key2 = 0b11110010;
        let key3 = 0b11110011;

        qf.insert(key1);
        qf.insert(key2);
        qf.insert(key3);

        assert!(qf.lookup(key1), "key1 should be found with wraparound");
        assert!(qf.lookup(key2), "key2 should be found with wraparound");
        assert!(qf.lookup(key3), "key3 should be found with wraparound");

        let non_existent = 0b11110100;
        assert!(
            !qf.lookup(non_existent),
            "non-existent key should not be found"
        );
    }

    #[test]
    fn test_lookup_with_insert_complex_pattern() {
        let mut qf = QuotientFilter::new(4, 4);

        let keys = vec![
            0b0001_0001,
            0b0001_0010,
            0b0010_0011,
            0b0010_0001,
            0b0011_0010,
            0b0001_0011,
            0b0100_0001,
        ];

        for &key in &keys {
            qf.insert(key);
        }

        for &key in &keys {
            assert!(qf.lookup(key), "inserted key {:08b} should be found", key);
        }

        let non_existent_keys = vec![
            0b0001_0100,
            0b0010_0010,
            0b0011_0001,
            0b0100_0010,
            0b0101_0001,
        ];

        for &key in &non_existent_keys {
            assert!(
                !qf.lookup(key),
                "non-existent key {:08b} should not be found",
                key
            );
        }
    }

    #[test]
    fn test_resize_rebuilds_filter() {
        let mut qf = QuotientFilter::new(3, 4);
        let keys = vec![
            0b0001_0001,
            0b0001_0010,
            0b0010_0011,
            0b0011_0100,
            0b0111_0101,
            0b0111_0101,
        ];

        for &key in &keys {
            qf.insert(key);
        }

        let old_size = qf.size;
        let old_entries = qf.entries;
        let old_q = qf.q;

        qf.resize();

        assert_eq!(qf.size, old_size * 2, "resize must double the table size");
        assert_eq!(qf.q, old_q + 1, "resize must increase q by one bit");
        assert_eq!(
            qf.entries, old_entries,
            "resize must preserve the number of stored entries"
        );

        for &key in &keys {
            assert!(
                qf.lookup(key),
                "key {:08b} should still be present after resize",
                key
            );
        }

        let new_key = 0b1000_0001;
        qf.insert(new_key);
        assert!(
            qf.lookup(new_key),
            "insert should continue to work after resize"
        );
        assert_eq!(
            qf.entries,
            old_entries + 1,
            "entry count should reflect the newly inserted element"
        );
    }
}
