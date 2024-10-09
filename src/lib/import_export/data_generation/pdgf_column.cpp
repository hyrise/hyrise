#include <vector>

#include "pdgf_column.hpp"

namespace hyrise {
template <typename T>
PDGFColumn<T>::PDGFColumn(ChunkOffset chunk_size, int64_t num_rows) : _chunk_size(chunk_size), _num_rows(num_rows) {
  for (auto row = int64_t{0}; row < _num_rows; row += _chunk_size) {
    auto chunk_vector = std::vector<T>{};
    chunk_vector.resize(std::min(static_cast<ChunkOffset>(_num_rows - row), chunk_size));
    _data_segments.emplace_back(std::move(chunk_vector));
  }
}

template <typename T>
void PDGFColumn<T>::add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;

  auto value = * reinterpret_cast<T*>(data);
  _data_segments[segment_index][segment_position] = std::move(value);
}

template<>
void PDGFColumn<pmr_string>::add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;

  auto value = pmr_string(data);
  _data_segments[segment_index][segment_position] = std::move(value);
}
} // namespace hyrise
