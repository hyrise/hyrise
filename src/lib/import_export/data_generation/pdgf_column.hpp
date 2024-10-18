#pragma once

#include <cstdint>
#include <vector>
#include <memory>

#include "abstract_pdgf_column.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {
template <typename T>
class PDGFColumn : public AbstractPDGFColumn {
 public:
  explicit PDGFColumn(int64_t num_rows, ChunkOffset chunk_size);
  void add(int64_t row, char* data) override;
  bool has_another_segment() override;
  std::shared_ptr<AbstractSegment> build_next_segment() override;

 protected:
  uint8_t _num_built_segments = 0;
  std::vector<pmr_vector<T>> _data_segments;
};

EXPLICITLY_INSTANTIATE_DATA_TYPES(PDGFColumn);
} // namespace hyrise
