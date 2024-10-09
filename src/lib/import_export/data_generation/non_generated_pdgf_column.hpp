#pragma once

#include <cstdint>
#include <memory>

#include "abstract_pdgf_column.hpp"
#include "all_type_variant.hpp"
#include "storage/abstract_segment.hpp"
#include "types.hpp"

namespace hyrise {
class NonGeneratedPDGFColumn : public AbstractPDGFColumn {
 public:
  explicit NonGeneratedPDGFColumn(DataType data_type, int64_t num_rows, ChunkOffset chunk_size);
  void add(int64_t row, char* data) override;
  bool has_another_segment() override;
  std::shared_ptr<AbstractSegment> build_next_segment() override;

 protected:
  DataType _data_type;

  uint8_t _num_built_segments = 0;
  uint8_t _total_segments;
};
} // namespace hyrise
