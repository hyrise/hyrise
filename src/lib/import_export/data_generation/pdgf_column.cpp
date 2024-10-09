#include "pdgf_column.hpp"

namespace hyrise {
template <typename T>
PDGFColumn<T>::PDGFColumn(int64_t num_rows, ChunkOffset chunk_size) : AbstractPDGFColumn( num_rows, chunk_size) {
  for (auto row = int64_t{0}; row < _num_rows; row += _chunk_size) {
    auto chunk_vector = pmr_vector<T>{};
    chunk_vector.resize(std::min(static_cast<ChunkOffset>(_num_rows - row), chunk_size));
    _data_segments.emplace_back(std::move(chunk_vector));
  }
}

template <typename T>
void PDGFColumn<T>::add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;
  _data_segments[segment_index][segment_position] = * reinterpret_cast<T*>(data);
}

template <typename T>
bool PDGFColumn<T>::has_another_segment() {
  return _num_built_segments < _data_segments.size();
}

template <typename T>
std::shared_ptr<AbstractSegment> PDGFColumn<T>::build_next_segment() {
  Assert(_num_built_segments < _data_segments.size(), "There are no segments left to build!");
  auto next_build_index = _num_built_segments;
  auto segment = std::make_shared<ValueSegment<T>>(std::move(_data_segments[next_build_index]));
  _num_built_segments++;
  return segment;
}


template<>
void PDGFColumn<pmr_string>::add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;

  _data_segments[segment_index][segment_position] = pmr_string(data);
}
} // namespace hyrise
