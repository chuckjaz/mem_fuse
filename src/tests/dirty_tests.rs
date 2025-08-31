use crate::dirty::DirtyRegions;

#[test]
fn test_add_region_non_overlapping() {
    let mut regions = DirtyRegions::new();
    regions.add_region(0, 10);
    regions.add_region(20, 30);
    assert_eq!(regions.regions().len(), 2);
    assert!(regions.regions().contains(&(0, 10)));
    assert!(regions.regions().contains(&(20, 30)));
}

#[test]
fn test_add_region_overlapping() {
    let mut regions = DirtyRegions::new();
    regions.add_region(0, 10);
    regions.add_region(5, 15);
    assert_eq!(regions.regions().len(), 1);
    assert!(regions.regions().contains(&(0, 15)));
}

#[test]
fn test_add_region_contained() {
    let mut regions = DirtyRegions::new();
    regions.add_region(0, 20);
    regions.add_region(5, 15);
    assert_eq!(regions.regions().len(), 1);
    assert!(regions.regions().contains(&(0, 20)));
}

#[test]
fn test_add_region_contains() {
    let mut regions = DirtyRegions::new();
    regions.add_region(5, 15);
    regions.add_region(0, 20);
    assert_eq!(regions.regions().len(), 1);
    assert!(regions.regions().contains(&(0, 20)));
}

#[test]
fn test_add_region_merges_multiple() {
    let mut regions = DirtyRegions::new();
    regions.add_region(0, 10);
    regions.add_region(20, 30);
    regions.add_region(5, 25);
    assert_eq!(regions.regions().len(), 1);
    assert!(regions.regions().contains(&(0, 30)));
}

#[test]
fn test_add_region_invalid() {
    let mut regions = DirtyRegions::new();
    regions.add_region(10, 0);
    assert!(regions.regions().is_empty());
    regions.add_region(10, 10);
    assert!(regions.regions().is_empty());
}

#[test]
fn test_remove() {
    let mut regions = DirtyRegions::new();
    regions.add_region(0, 10);
    regions.add_region(20, 30);
    regions.remove_region(0, 10);
    regions.remove_region(20, 30);
    assert!(regions.regions().is_empty());
}
