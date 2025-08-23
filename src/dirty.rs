use std::collections::BTreeSet;

#[derive(Debug, Default, Clone)]
pub struct DirtyRegions {
    regions: BTreeSet<(u64, u64)>,
}

impl DirtyRegions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_region(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        let mut new_regions = BTreeSet::new();
        let mut merged_start = start;
        let mut merged_end = end;

        for &(r_start, r_end) in self.regions.iter() {
            if r_end < start || r_start > end {
                new_regions.insert((r_start, r_end));
            } else {
                merged_start = merged_start.min(r_start);
                merged_end = merged_end.max(r_end);
            }
        }

        new_regions.insert((merged_start, merged_end));
        self.regions = new_regions;
    }

    pub fn regions(&self) -> &BTreeSet<(u64, u64)> {
        &self.regions
    }

    pub fn clear(&mut self) {
        self.regions.clear();
    }

    pub fn truncate(&mut self, size: u64) {
        let mut new_regions = BTreeSet::new();
        for &(start, end) in self.regions.iter() {
            if start < size {
                new_regions.insert((start, end.min(size)));
            }
        }
        self.regions = new_regions;
    }
}
