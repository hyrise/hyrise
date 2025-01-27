#pragma once

#include <cstdint>

#include "encoding_config.hpp"
#include "multi_process_ring_buffer.hpp"
#include "pdgf_table_builder.hpp"
#include "pdgf_table_schema_builder.hpp"
#include "shared_memory_dto.hpp"
#include "types.hpp"

namespace hyrise {
class BaseSharedMemoryReader : Noncopyable {
 public:
  virtual ~BaseSharedMemoryReader() = default;

  virtual bool has_next_table() const = 0;
  virtual std::unique_ptr<BasePDGFTableSchemaBuilder> read_next_schema() = 0;
  virtual std::shared_ptr<BasePDGFTableBuilder> read_next_table(const EncodingConfig& encoding_config, uint32_t num_workers) = 0;
  virtual void reset() = 0;
};

std::shared_ptr<BaseSharedMemoryReader> create_shared_memory_reader(
 uint32_t work_unit_size, uint32_t reader_num_columns, ChunkOffset hyrise_table_chunk_size,
 const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem);

template <uint32_t work_unit_size, uint32_t num_columns>
class SharedMemoryReader : public BaseSharedMemoryReader {
 public:
  static const uint32_t buffer_size = SHARED_MEMORY_DATA_SLOTS / work_unit_size / num_columns;

  explicit SharedMemoryReader(ChunkOffset hyrise_table_chunk_size, const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem);
  ~SharedMemoryReader() override;

  bool has_next_table() const override;
  std::unique_ptr<BasePDGFTableSchemaBuilder> read_next_schema() override;
  std::shared_ptr<BasePDGFTableBuilder> read_next_table(const EncodingConfig& encoding_config, uint32_t num_workers) override;
  void reset() override;

 protected:
  void _return_data_slot(uint32_t buffer_offset);
  void _worker_read_data(uint32_t reader_index, std::shared_ptr<PDGFTableBuilder<work_unit_size, num_columns>>& table_builder);

  ChunkOffset _hyrise_table_chunk_size;
  const char* _shared_memory_file_name;
  int _shm_fd;
  DataBuffer<buffer_size, work_unit_size, num_columns>* _data_buffer;
  std::shared_ptr<MultiProcessRingBuffer<buffer_size>> _ring_buffer;

  int32_t _num_tables_to_read = INT32_MAX;
  int32_t _num_read_tables = 0;
};
} // namespace hyrise
