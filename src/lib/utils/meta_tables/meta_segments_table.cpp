#include "meta_segments_table.hpp"

#include "hyrise.hpp"
#include "utils/meta_tables/segment_meta_data.hpp"

namespace opossum {

MetaSegmentsTable::MetaSegmentsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"chunk_id", DataType::Int, false},
                                               {"column_id", DataType::Int, false},
                                               {"column_name", DataType::String, false},
                                               {"column_data_type", DataType::String, false},
                                               {"encoding_type", DataType::String, true},
                                               {"vector_compression_type", DataType::String, true},
                                               {"estimated_size_in_bytes", DataType::Long, false}}) {}

const std::string& MetaSegmentsTable::name() const {
  static const auto name = std::string{"segments"};
  return name;
}

std::shared_ptr<Table> MetaSegmentsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Sampled);

  return output_table;
}

}  // namespace opossum
