class SecondaryScanBuilder {
 public:
  SecondaryScanBuilder(OperatorScanPredicate& predicate) : _predicate(predicate) {}

  std::function<bool(const ChunkOffset)> build(const Table& table, const ChunkId chunk_id) {}

 private:
  const OperatorScanPredicate& _predicate
};

std::vector<> secondary_scan_builders;