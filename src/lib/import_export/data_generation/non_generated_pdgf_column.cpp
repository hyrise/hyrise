#include "non_generated_pdgf_column.hpp"
#include "storage/dummy_segment.hpp"

namespace hyrise {
NonGeneratedPDGFColumn::NonGeneratedPDGFColumn(DataType data_type, int64_t num_rows, ChunkOffset chunk_size) : AbstractPDGFColumn(num_rows, chunk_size), _data_type(data_type), _total_segments((num_rows / chunk_size) + 1) {}

void NonGeneratedPDGFColumn::add(int64_t row, char* data) {
  throw std::logic_error("Cannot add data to non-generated column!");
}

bool NonGeneratedPDGFColumn::has_another_segment() {
  return _num_built_segments < _total_segments;
}

std::shared_ptr<AbstractSegment> NonGeneratedPDGFColumn::build_next_segment() {
  auto chunk_size = std::min(_chunk_size, static_cast<ChunkOffset>(_num_rows - _total_segments * _num_built_segments));
  auto segment = std::make_shared<DummySegment>(_data_type, chunk_size);
  _num_built_segments++;
  return segment;
}
} // namespace hyrise
