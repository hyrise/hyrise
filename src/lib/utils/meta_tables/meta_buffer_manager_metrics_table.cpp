#include "meta_buffer_manager_metrics_table.hpp"
#include "hyrise.hpp"

namespace hyrise {

MetaBufferManagerMetricsTable::MetaBufferManagerMetricsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"current_bytes_used_dram", DataType::Int, false},
                                               {"current_bytes_used_numa", DataType::Int, false},
                                               {"total_allocated_bytes_dram", DataType::Int, false},
                                               {"total_allocated_bytes_numa", DataType::Int, false},
                                               {"total_unused_bytes_numa", DataType::Int, false},
                                               {"total_unused_bytes_dram", DataType::Int, false},
                                               {"internal_fragmentation_rate_dram", DataType::Double, false},
                                               {"internal_fragmentation_rate_numa", DataType::Double, false},
                                               {"num_allocs", DataType::Int, false},
                                               {"num_deallocs", DataType::Int, false},
                                               {"total_bytes_copied_from_ssd_to_dram", DataType::Int, false},
                                               {"total_bytes_copied_from_ssd_to_numa", DataType::Int, false},
                                               {"total_bytes_copied_from_numa_to_dram", DataType::Int, false},
                                               {"total_bytes_copied_from_dram_to_numa", DataType::Int, false},
                                               {"total_bytes_copied_from_dram_to_ssd", DataType::Int, false},
                                               {"total_bytes_copied_from_numa_to_ssd", DataType::Int, false},
                                               {"total_bytes_copied_to_ssd", DataType::Int, false},
                                               {"total_bytes_copied_from_ssd", DataType::Int, false},
                                               {"total_hits_dram", DataType::Int, false},
                                               {"total_hits_numa", DataType::Int, false},
                                               {"total_misses_dram", DataType::Int, false},
                                               {"total_misses_numa", DataType::Int, false},
                                               {"total_pins_dram", DataType::Int, false},
                                               {"current_pins_dram", DataType::Int, false},
                                               {"total_pins_numa", DataType::Int, false},
                                               {"current_pins_numa", DataType::Int, false},
                                               {"num_dram_eviction_queue_items_purged", DataType::Int, false},
                                               {"num_dram_eviction_queue_adds", DataType::Int, false},
                                               {"num_numa_eviction_queue_items_purged", DataType::Int, false},
                                               {"num_numa_eviction_queue_adds", DataType::Int, false},
                                               {"num_dram_evictions", DataType::Int, false},
                                               {"num_numa_evictions", DataType::Int, false},
                                               {"num_madvice_free_calls_numa", DataType::Int, false},
                                               {"num_madvice_free_calls_dram", DataType::Int, false}}) {}

const std::string& MetaBufferManagerMetricsTable::name() const {
  static const auto name = std::string{"buffer_manager_metrics"};
  return name;
}

std::shared_ptr<Table> MetaBufferManagerMetricsTable::_on_generate() const {
  const auto metrics = Hyrise::get().buffer_manager.metrics();
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  output_table->append({static_cast<int64_t>(metrics.current_bytes_used_dram),
                        static_cast<int64_t>(metrics.current_bytes_used_numa),
                        static_cast<int64_t>(metrics.total_allocated_bytes_dram),
                        static_cast<int64_t>(metrics.total_allocated_bytes_numa),
                        static_cast<int64_t>(metrics.total_unused_bytes_numa),
                        static_cast<int64_t>(metrics.total_unused_bytes_dram),
                        metrics.internal_fragmentation_rate_dram(),
                        metrics.internal_fragmentation_rate_numa(),
                        static_cast<int64_t>(metrics.num_allocs),
                        static_cast<int64_t>(metrics.num_deallocs),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_ssd_to_dram),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_ssd_to_numa),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_numa_to_dram),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_dram_to_numa),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_dram_to_ssd),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_numa_to_ssd),
                        static_cast<int64_t>(metrics.total_bytes_copied_to_ssd),
                        static_cast<int64_t>(metrics.total_bytes_copied_from_ssd),
                        static_cast<int64_t>(metrics.total_hits_dram),
                        static_cast<int64_t>(metrics.total_hits_numa),
                        static_cast<int64_t>(metrics.total_misses_dram),
                        static_cast<int64_t>(metrics.total_misses_numa),
                        static_cast<int64_t>(metrics.total_pins_dram),
                        static_cast<int64_t>(metrics.current_pins_dram),
                        static_cast<int64_t>(metrics.total_pins_numa),
                        static_cast<int64_t>(metrics.current_pins_numa),
                        static_cast<int64_t>(metrics.num_dram_eviction_queue_items_purged),
                        static_cast<int64_t>(metrics.num_dram_eviction_queue_adds),
                        static_cast<int64_t>(metrics.num_numa_eviction_queue_items_purged),
                        static_cast<int64_t>(metrics.num_numa_eviction_queue_adds),
                        static_cast<int64_t>(metrics.num_dram_evictions),
                        static_cast<int64_t>(metrics.num_numa_evictions),
                        static_cast<int64_t>(metrics.num_madvice_free_calls_numa),
                        static_cast<int64_t>(metrics.num_madvice_free_calls_dram)});

  return output_table;
}
}  // namespace hyrise
