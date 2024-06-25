#include "meta_segments_accurate_table.hpp"

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"
#include "utils/meta_tables/segment_meta_data.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

MetaSegmentsAccurateTable::MetaSegmentsAccurateTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"chunk_id", DataType::Int, false},
                                               {"column_id", DataType::Int, false},
                                               {"column_name", DataType::String, false},
                                               {"column_data_type", DataType::String, false},
                                               {"distinct_value_count", DataType::Long, false},
                                               {"encoding_type", DataType::String, true},
                                               {"vector_compression_type", DataType::String, true},
                                               {"size_in_bytes", DataType::Long, false},
                                               {"point_accesses", DataType::Long, false},
                                               {"sequential_accesses", DataType::Long, false},
                                               {"monotonic_accesses", DataType::Long, false},
                                               {"random_accesses", DataType::Long, false},
                                               {"dictionary_accesses", DataType::Long, false}}) {}

const std::string& MetaSegmentsAccurateTable::name() const {
  static const auto name = std::string{"segments_accurate"};
  return name;
}

std::shared_ptr<Table> MetaSegmentsAccurateTable::_on_generate() const {
  PerformanceWarning("Accurate segment information are expensive to gather. Use with caution.");

  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Full);

  return output_table;
}

}  // namespace hyrise
