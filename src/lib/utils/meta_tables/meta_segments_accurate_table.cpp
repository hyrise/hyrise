#include "meta_segments_accurate_table.hpp"

#include "hyrise.hpp"
#include "utils/meta_tables/segment_meta_data.hpp"

namespace opossum {

MetaSegmentsAccurateTable::MetaSegmentsAccurateTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"chunk_id", DataType::Int, false},
                                               {"column_id", DataType::Int, false},
                                               {"column_name", DataType::String, false},
                                               {"column_data_type", DataType::String, false},
                                               {"distinct_value_count", DataType::Long, false},
                                               {"encoding_type", DataType::String, true},
                                               {"vector_compression_type", DataType::String, true},
                                               {"size_in_bytes", DataType::Long, false}}) {}

const std::string& MetaSegmentsAccurateTable::name() const {
  static const auto name = std::string{"segments_accurate"};
  return name;
}

std::shared_ptr<Table> MetaSegmentsAccurateTable::_on_generate() const {
  PerformanceWarning("Accurate segment information are expensive to gather. Use with caution.");

  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Full);

  return output_table;
}

}  // namespace opossum
