#pragma once

#include <atomic>
#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>

#include "abstract_pdgf_column.hpp"
#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"
#include "scheduler/job_task.hpp"

namespace hyrise {
class BasePDGFColumn {
 public:
  explicit BasePDGFColumn(DataType data_type, int64_t num_rows, ChunkOffset chunk_size);
  explicit BasePDGFColumn(DataType data_type);
  virtual ~BasePDGFColumn() = default;
  virtual void virtual_add(int64_t row, char* data) = 0;
  virtual void values_added(int64_t start_row, size_t num_values) = 0;
  virtual ChunkID num_segments() = 0;
  virtual std::shared_ptr<AbstractSegment> obtain_segment(ChunkID chunk_id) = 0;
 protected:
  DataType _data_type;
  int64_t _num_rows;
  ChunkOffset _chunk_size;
};

template <typename T>
struct SegmentToConstruct {
 pmr_vector<T> raw_data;
 std::shared_ptr<AbstractSegment> finished_segment;
 std::atomic_uint32_t _num_entries;
};

template <typename T>
class PDGFColumn : public BasePDGFColumn {
 public:
  explicit PDGFColumn(DataType data_type, SegmentEncodingSpec encoding_spec, int64_t num_rows, ChunkOffset chunk_size);
  void virtual_add(int64_t row, char* data) override;
  void values_added(int64_t start_row, size_t num_values) override;
  ChunkID num_segments() override;
  void initialize_segment_if_necessary(ChunkID chunk_id, bool should_try_continue_initialization);
  void build_segment(ChunkID chunk_id);
  std::shared_ptr<AbstractSegment> obtain_segment(ChunkID chunk_id) override;

 protected:
  SegmentEncodingSpec _encoding_spec;
  std::vector<bool> _segment_initialized;
  std::vector<std::shared_ptr<std::mutex>> _segment_initialization_locks;
  std::vector<pmr_vector<T>> _data_segments;
  std::vector<std::shared_ptr<JobTask>> _encoding_tasks;
  std::vector<std::shared_ptr<AbstractSegment>> _finished_segments;
  std::vector<std::shared_ptr<std::atomic_uint32_t>> _segment_fullness;
};
} // namespace hyrise
