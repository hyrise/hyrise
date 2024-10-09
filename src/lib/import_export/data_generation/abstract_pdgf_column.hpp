#pragma once

#include <cstdint>
#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "storage/abstract_segment.hpp"

namespace hyrise {
enum ColumnType: uint32_t { STRING = 0, INTEGER = 1, LONG = 2, DOUBLE = 3, BOOL = 4 };

DataType hyrise_type_for_column_type(ColumnType column_type);

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
} // namespace hyrise
