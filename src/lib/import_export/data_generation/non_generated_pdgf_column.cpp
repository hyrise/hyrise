#include "non_generated_pdgf_column.hpp"
#include "storage/dummy_segment.hpp"

namespace hyrise {
template <typename T>
NonGeneratedPDGFColumn<T>::NonGeneratedPDGFColumn(int64_t num_rows, ChunkOffset chunk_size) : AbstractPDGFColumn(num_rows, chunk_size), _total_segments((num_rows / chunk_size) + 1) {}

template <typename T>
void NonGeneratedPDGFColumn<T>::add(int64_t row, char* data) {
  throw std::logic_error("Cannot add data to non-generated column!");
}

template <typename T>
bool NonGeneratedPDGFColumn<T>::has_another_segment() {
  return _num_built_segments < _total_segments;
}

template <typename T>
std::shared_ptr<AbstractSegment> NonGeneratedPDGFColumn<T>::build_next_segment() {
  auto chunk_size = std::min(_chunk_size, static_cast<ChunkOffset>(_num_rows - _total_segments * _num_built_segments));
  auto segment = std::make_shared<DummySegment<T>>(chunk_size);
  _num_built_segments++;
  return segment;
}
} // namespace hyrise
