#pragma once

#include <cstdint>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {
class AbstractPDGFColumn: private Noncopyable {
 public:
  virtual ~AbstractPDGFColumn() = default;
  virtual void add(int64_t row, char* data) = 0;
};

template <typename T>
class PDGFColumn : public AbstractPDGFColumn {
 public:
  explicit PDGFColumn(ChunkOffset chunk_size, int64_t num_rows);
  void add(int64_t row, char* data);

 protected:
  ChunkOffset _chunk_size;
  int64_t _num_rows;
  std::vector<std::vector<T>> _data_segments;
};

EXPLICITLY_INSTANTIATE_DATA_TYPES(PDGFColumn);
} // namespace hyrise
