use murmurhash3::murmurhash3_x86_32 as mmh3;

pub struct CountMinSketch {
    #[allow(dead_code)]
    eps: f32,
    #[allow(dead_code)]
    delta: f32,
    width: usize,
    depth: usize,
    sketch: Vec<Vec<u32>>,
}

impl CountMinSketch {
    pub fn new(eps: f32, delta: f32) -> Self {
        let width = (std::f32::consts::E / eps).ceil() as usize;
        let depth = (1.0_f32 / delta).ln().ceil() as usize;
        let sketch = vec![vec![0u32; width]; depth];
        CountMinSketch {
            eps,
            delta,
            width,
            depth,
            sketch,
        }
    }

    pub fn update(&mut self, item: &[u8], freq: u32) {
        for i in 0..self.depth {
            let index = mmh3(item, i as u32) % self.width as u32;
            self.sketch[i][index as usize] += freq;
        }
    }

    pub fn estimate(&self, item: &[u8]) -> u32 {
        let mut min = u32::MAX;
        for i in 0..self.depth {
            let index = mmh3(item, i as u32) % self.width as u32;
            if self.sketch[i][index as usize] < min {
                min = self.sketch[i][index as usize];
            }
        }
        min
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_expected_dimensions_and_initial_state() {
        let cms = CountMinSketch::new(0.01, 0.1);
        assert_eq!(cms.width, (std::f32::consts::E / 0.01).ceil() as usize);
        assert_eq!(cms.depth, (1.0_f32 / 0.1).ln().ceil() as usize);
        assert!(cms.sketch.iter().all(|row| row.len() == cms.width));
        assert!(cms
            .sketch
            .iter()
            .flat_map(|row| row.iter())
            .all(|&count| count == 0));
    }

    #[test]
    fn estimate_returns_zero_before_any_updates() {
        let cms = CountMinSketch::new(0.01, 0.1);
        assert_eq!(cms.estimate(b"unknown"), 0);
    }

    #[test]
    fn updates_accumulate_frequency_for_same_item() {
        let mut cms = CountMinSketch::new(0.01, 0.1);
        cms.update(b"key", 4);
        cms.update(b"key", 6);
        assert_eq!(cms.estimate(b"key"), 10);
    }

    #[test]
    fn update_with_zero_frequency_keeps_counts_unchanged() {
        let mut cms = CountMinSketch::new(0.01, 0.1);
        cms.update(b"key", 5);
        let before = cms.estimate(b"key");
        cms.update(b"key", 0);
        assert_eq!(cms.estimate(b"key"), before);
    }

    #[test]
    fn handles_empty_items() {
        let mut cms = CountMinSketch::new(0.01, 0.1);
        cms.update(b"", 3);
        assert_eq!(cms.estimate(b""), 3);
        assert_eq!(cms.estimate(b"non-empty"), 0);
    }

    #[test]
    fn collisions_do_not_underestimate_counts() {
        let mut cms = CountMinSketch::new(std::f32::consts::E, 0.1);
        assert_eq!(cms.width, 1);

        cms.update(b"alpha", 5);
        cms.update(b"beta", 3);

        let alpha_estimate = cms.estimate(b"alpha");
        let beta_estimate = cms.estimate(b"beta");

        assert_eq!(alpha_estimate, 8);
        assert_eq!(beta_estimate, 8);
    }

    #[test]
    fn depth_is_one_when_delta_close_to_one() {
        let cms = CountMinSketch::new(0.01, 0.9);
        assert_eq!(cms.depth, 1);
    }
}
