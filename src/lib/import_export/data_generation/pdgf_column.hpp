#pragma once

#include <cstdint>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {
enum ColumnType: uint32_t { STRING = 0, INTEGER = 1, LONG = 2, FLOAT = 3, BOOL = 4 };

class AbstractPDGFColumn: private Noncopyable {
 public:
  explicit AbstractPDGFColumn(int64_t num_rows, ChunkOffset chunk_size);
  virtual ~AbstractPDGFColumn() = default;
  virtual void add(int64_t row, char* data) = 0;
  virtual bool has_another_segment() = 0;
  virtual std::shared_ptr<AbstractSegment> build_next_segment() = 0;

 protected:
  int64_t _num_rows;
  ChunkOffset _chunk_size;
};

class NonGeneratedPDGFColumn : public AbstractPDGFColumn {
 public:
  explicit NonGeneratedPDGFColumn(int64_t num_rows, ChunkOffset chunk_size);
  void add(int64_t row, char* data);
  bool has_another_segment();
  std::shared_ptr<AbstractSegment> build_next_segment();

 protected:
  uint8_t _num_built_segments = 0;
  uint8_t _total_segments;
};

template <typename T>
class PDGFColumn : public AbstractPDGFColumn {
 public:
  explicit PDGFColumn(int64_t num_rows, ChunkOffset chunk_size);
  void add(int64_t row, char* data);
  bool has_another_segment();
  std::shared_ptr<AbstractSegment> build_next_segment();

 protected:
  uint8_t _num_built_segments = 0;
  std::vector<pmr_vector<T>> _data_segments;
};

EXPLICITLY_INSTANTIATE_DATA_TYPES(PDGFColumn);
} // namespace hyrise
