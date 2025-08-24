use crate::invariant_files_mirror::{ContentInformation, FileDirectoryEntry};

#[test]
fn test_entries_deserialization() -> std::io::Result<()> {
    let data = "[{\"name\":\".gitignore\",\"info\":{\"node\":2,\"kind\":\"File\",\"modifyTime\":1721419791909,\"createTime\":1721419791909,\"executable\":false,\"writable\":false,\"etag\":\"aae8e9997fb040fb78109337af8a9ee31d6649d7280a9f7fa61e3bcb7854709f\",\"size\":34}}]";
    let entries:  Vec<FileDirectoryEntry>  = serde_json::from_str(data)?;
    let entry = &entries[0];
    assert_eq!(entry.name, ".gitignore");
    if let ContentInformation::File(info) = &entry.info {
        assert_eq!(info.size, 34);
    }
    Ok(())
}
