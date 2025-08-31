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

    pub fn is_dirty(&self, start: u64, end: u64) -> bool {
        for (dirty_start, dirty_end) in self.regions.iter() {
            if end <= *dirty_start { return false }
            if start < *dirty_start && end >= *dirty_end { return true }
            if start >= *dirty_start && start < *dirty_end { return true }
            if end > *dirty_start && end <= *dirty_end { return true }
        }
        return false
    }

    pub fn regions(&self) -> &BTreeSet<(u64, u64)> {
        &self.regions
    }

    pub fn remove_region(&mut self, start: u64, end: u64) {
        let _ = self.regions.remove(&(start, end));
    }
}
