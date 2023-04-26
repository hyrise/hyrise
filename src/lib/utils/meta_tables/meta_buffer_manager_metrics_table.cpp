#include "meta_buffer_manager_metrics_table.hpp"
#include "hyrise.hpp"

namespace hyrise {

//     std::size_t total_bytes_copied_from_ssd_to_dram = 0;
//     std::size_t total_bytes_copied_from_ssd_to_numa = 0;

//     std::size_t total_bytes_copied_from_numa_to_dram = 0;
//     std::size_t total_bytes_copied_from_dram_to_numa = 0;
//     std::size_t total_bytes_copied_from_dram_to_ssd = 0;
//     std::size_t total_bytes_copied_from_numa_to_ssd = 0;
//     std::size_t total_bytes_copied_to_ssd = 0;
//     std::size_t total_bytes_copied_from_ssd = 0;

MetaBufferManagerMetricsTable::MetaBufferManagerMetricsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"total_bytes_copied_from_ssd_to_dram", DataType::Int, false},
                                               {"total_bytes_copied_from_ssd_to_numa", DataType::Int, false},
                                               {"total_bytes_copied_from_numa_to_dram", DataType::Int, false},
                                               {"total_bytes_copied_from_dram_to_numa", DataType::Int, false},
                                               {"total_bytes_copied_from_dram_to_ssd", DataType::Int, false},
                                               {"total_bytes_copied_from_numa_to_ssd", DataType::Int, false},
                                               {"total_bytes_copied_from_ssd", DataType::Int, false},
                                               {"total_bytes_copied_from_ssd", DataType::Int, false}}) {}

const std::string& MetaBufferManagerMetricsTable::name() const {
  static const auto name = std::string{"buffer_manager_metrics"};
  return name;
}

std::shared_ptr<Table> MetaBufferManagerMetricsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  output_table->append({static_cast<int64_t>(total_bytes_copied_from_ssd_to_dram)});

  return output_table;
}
}  // namespace hyrise
