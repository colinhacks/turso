use super::*;
use crate::{alloc::TursoVecInExt, index_method::IndexMethod, schema::IndexColumn};
use turso_parser::ast::{Expr, Literal, Name};

#[test]
fn field_weights_accept_finite_positive_boundaries() {
    let columns = [
        IndexColumn::new("title", 0, crate::translate::collate::CollationSeq::BINARY),
        IndexColumn::new("body", 1, crate::translate::collate::CollationSeq::BINARY),
    ];
    for (input, expected) in [
        ("1e-45", f32::from_bits(1)),
        ("1.17549435e-38", f32::MIN_POSITIVE),
        ("0.5", 0.5),
        ("+1", 1.0),
        ("3.4028235e38", f32::MAX),
    ] {
        let weights = parse_field_weights(&format!("title={input},body=2"), &columns).unwrap();
        assert_eq!(weights["title"], expected, "{input}");
        assert_eq!(weights["body"], 2.0);
    }
}

#[test]
fn field_weights_rename_column() {
    let rename = |weights: &str, to: &str| -> Result<String> {
        let mut with_clause = vec![(
            Name::exact("WEIGHTS".to_string()),
            Box::new(Expr::Literal(Literal::String(weights.to_string()))),
        )];
        FtsIndexMethod.rename_column_in_with_clause(&mut with_clause, "title", to)?;
        let Expr::Literal(Literal::String(weights)) = *with_clause.remove(0).1 else {
            panic!("weights must stay a string literal");
        };
        Ok(weights)
    };
    assert_eq!(
        rename("'Title=2.0, body=1.5'", "Heading").unwrap(),
        "'Heading=2.0,body=1.5'"
    );
    assert_eq!(rename("'body = 1.5'", "heading").unwrap(), "'body = 1.5'");
    for to in ["a,b", "a=b", " a", "a ", "it's"] {
        assert!(rename("'title=2.0'", to).is_err(), "accepted {to:?}");
    }
}

#[test]
fn chunk_assembly_rejects_stray_chunk_numbers_without_panicking() {
    let path = std::path::Path::new("x.term");
    let mut chunks: HashMap<i64, Vec<u8>> = HashMap::default();
    chunks.insert(0, vec![1, 2, 3]);
    chunks.insert(1, vec![4, 5]);
    assert_eq!(
        assemble_chunks(path, chunks.clone(), &DynAllocator::default())
            .unwrap()
            .as_slice(),
        &[1, 2, 3, 4, 5]
    );

    // A negative chunk number next to valid ones: it is counted but never
    // written, so assembly must error rather than hand out uninitialized
    // bytes or trip an assert.
    chunks.insert(-1, vec![9]);
    assert!(matches!(
        assemble_chunks(path, chunks.clone(), &DynAllocator::default()),
        Err(LimboError::Corrupt(_))
    ));

    // A hole is reported as the missing chunk.
    chunks.remove(&-1);
    chunks.remove(&0);
    assert!(matches!(
        assemble_chunks(path, chunks, &DynAllocator::default()),
        Err(LimboError::Corrupt(_))
    ));
}

#[test]
fn snapshot_directory_reads_shared_file_bytes_without_copying() {
    use tantivy::directory::Directory;

    let path = std::path::Path::new("segment.term");
    let mut bytes =
        <DynVec<u8> as TursoVecInExt<u8, DynAllocator>>::new_in(DynAllocator::default());
    bytes.try_extend([3, 5, 7, 11, 13]).unwrap();
    let data = Arc::new(bytes);
    let files = HashMap::from_iter([(path.to_path_buf(), Arc::clone(&data))]);
    let mut meta_json =
        <DynVec<u8> as TursoVecInExt<u8, DynAllocator>>::new_in(DynAllocator::default());
    meta_json.try_extend(b"metadata".iter().copied()).unwrap();
    let directory = SnapshotDirectory::new(files, meta_json);
    assert!(directory.exists(path).unwrap());
    assert!(directory.exists(std::path::Path::new("meta.json")).unwrap());
    assert!(!directory
        .exists(std::path::Path::new("missing.term"))
        .unwrap());
    assert_eq!(directory.atomic_read(path).unwrap(), &[3, 5, 7, 11, 13]);
    assert_eq!(
        directory
            .atomic_read(std::path::Path::new("meta.json"))
            .unwrap(),
        b"metadata"
    );
    let handle = directory.get_file_handle(path).unwrap();
    let read = handle.read_bytes(1..4).unwrap();

    assert_eq!(read.as_slice(), &[5, 7, 11]);
    assert_eq!(read.as_slice().as_ptr(), data[1..].as_ptr());
    let meta = directory
        .get_file_handle(std::path::Path::new("meta.json"))
        .unwrap()
        .read_bytes(1..5)
        .unwrap();
    assert_eq!(meta.as_slice(), b"etad");
    let other_meta = directory
        .get_file_handle(std::path::Path::new("meta.json"))
        .unwrap()
        .read_bytes(1..5)
        .unwrap();
    assert_eq!(meta.as_slice().as_ptr(), other_meta.as_slice().as_ptr());
}

#[test]
fn query_limit_is_exact_and_bounded_by_live_documents() {
    assert_eq!(bounded_query_limit(None, 1_500_000), 1_500_000);
    assert_eq!(bounded_query_limit(Some(-1), 1_500_000), 1_500_000);
    assert_eq!(bounded_query_limit(Some(i64::MAX), 37), 37);
    assert_eq!(bounded_query_limit(Some(12), 37), 12);
    assert_eq!(bounded_query_limit(Some(0), 37), 0);
    assert_eq!(bounded_query_limit(None, 0), 0);
}

#[test]
fn segment_byte_cache_keeps_newest_and_respects_budget() {
    let mut cache = SegmentByteCache::default();
    let make_data = |bytes: usize| {
        let mut files = HashMap::default();
        let mut data = DynVec::try_with_capacity_in(bytes, DynAllocator::default()).unwrap();
        data.try_extend(std::iter::repeat_n(0u8, bytes)).unwrap();
        files.insert("f".to_string(), Arc::new(data));
        Arc::new(SegmentData::new(files, SegmentIdentities::new(Vec::new())))
    };
    let a = SegmentId::generate_random();
    let b = SegmentId::generate_random();
    let c = SegmentId::generate_random();
    cache.put(a, make_data(100), 250);
    cache.put(b, make_data(100), 250);
    cache.put(c, make_data(100), 250);
    assert!(cache.get(&a).is_none(), "oldest entry evicted over budget");
    assert!(cache.get(&b).is_some());
    assert!(cache.get(&c).is_some());

    // An entry larger than the whole budget is still kept (it is the
    // newest); older entries are evicted to make room.
    cache.put(a, make_data(1000), 250);
    assert!(cache.get(&a).is_some());
    assert!(cache.get(&b).is_none());
    assert!(cache.get(&c).is_none());

    cache.put(b, make_data(100), 0);
    assert!(cache.get(&a).is_none());
    assert!(cache.get(&b).is_none());
}

#[test]
fn segment_byte_cache_counts_spare_file_capacity() {
    let mut bytes = DynVec::try_with_capacity_in(128, DynAllocator::default()).unwrap();
    bytes.try_extend([1, 2]).unwrap();
    let files = HashMap::from_iter([("f".to_string(), Arc::new(bytes))]);
    let segment = SegmentData::new(files, SegmentIdentities::new(Vec::new()));

    assert_eq!(segment.files["f"].len(), 2);
    assert_eq!(segment.total_bytes, 128);
}

#[test]
fn fts_write_errors_do_not_infer_out_of_memory_from_the_message() {
    let directory = BuildDirectory::default();
    let other = std::io::Error::other("memory allocation failed");
    assert!(matches!(
        directory.write_error(other.into(), "FTS build"),
        LimboError::InternalError(message) if message == "FTS build: An IO error occurred: 'memory allocation failed'"
    ));
}
