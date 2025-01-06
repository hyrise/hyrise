#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <array>
#include <atomic>
#include <functional>

#include "all_type_variant.hpp"
#include "encoding_config.hpp"
#include "shared_memory_dto.hpp"
#include "pdgf_column.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {
class BasePDGFTableBuilder : Noncopyable {
 public:
  explicit BasePDGFTableBuilder() = default;
  virtual ~BasePDGFTableBuilder() = default;

  virtual std::string table_name() const = 0;
  virtual std::shared_ptr<Table> build_table() = 0;
};

template <uint32_t work_unit_size, uint32_t num_columns>
class PDGFTableBuilder : public BasePDGFTableBuilder {
 public:
  explicit PDGFTableBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size);

  bool reader_should_handle_another_work_unit();
  bool reading_should_be_parallelized() const;

  std::string table_name() const override;
  std::shared_ptr<Table> build_table() override;

  void read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell, const EncodingConfig &encoding_config);
  void read_data(uint32_t table_id, int64_t sorting_id, SharedMemoryDataCell<work_unit_size, num_columns>* data_cell);

 protected:
  void _new_column_with_data_type(uint8_t target_index, SegmentEncodingSpec segment_encoding_spec, DataType data_type);

  ChunkEncodingSpec _encoding_spec;
  ChunkOffset _hyrise_table_chunk_size;

  uint32_t _table_id;
  std::string _table_name;
  int64_t _table_num_rows;
  std::atomic_int64_t _remaining_work_units_to_read;

  uint8_t _num_generated_columns;
  std::array<std::shared_ptr<BasePDGFColumn>, num_columns> _generated_columns;
  std::array<ColumnID, num_columns> _generated_column_mappings;
};
} // namespace hyrise