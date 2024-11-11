#pragma once

#include <cstdint>
#include <vector>
#include <memory>

#include "abstract_pdgf_column.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {
class BasePDGFColumn {
 public:
  explicit BasePDGFColumn(int64_t num_rows, ChunkOffset chunk_size);
  explicit BasePDGFColumn();
  virtual ~BasePDGFColumn();
  virtual void virtual_add(int64_t row, char* data) = 0;
  virtual bool has_another_segment() = 0;
  virtual std::shared_ptr<AbstractSegment> build_next_segment() = 0;
 protected:
  int64_t _num_rows;
  ChunkOffset _chunk_size;
};

template <typename T>
class PDGFColumn : public BasePDGFColumn {
 public:
  explicit PDGFColumn(int64_t num_rows, ChunkOffset chunk_size);
  static void call_add(std::shared_ptr<BasePDGFColumn>& self, int64_t row, char* data);
  void virtual_add(int64_t row, char* data) override;
  void add(int64_t row, char* data);
  bool has_another_segment() override;
  std::shared_ptr<AbstractSegment> build_next_segment() override;

 protected:
  uint32_t _num_built_segments = 0;
  std::vector<pmr_vector<T>> _data_segments;
};
} // namespace hyrise
