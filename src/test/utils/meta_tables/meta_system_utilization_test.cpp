#include "base_test.hpp"

#include "utils/meta_tables/meta_system_utilization_table.hpp"

namespace opossum {

class MetaSystemUtilizationTest : public BaseTest {
 protected:
  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) {
    return table->_generate();
  }
};

TEST_F(MetaSystemUtilizationTest, ProcessVirtualMemoryUsage) {
  const auto meta_system_utilization_table = std::make_shared<MetaSystemUtilizationTable>();

  // idle measurement
  const auto system_utilization_idle = generate_meta_table(meta_system_utilization_table);

  // create dummy table
  const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"dummy_column", DataType::Long, false}}, TableType::Data, 5);
  const auto dummy_table_values = 1024 * 1024 / 64;
  for (uint64_t value = 0; value < dummy_table_values; ++value) {
    dummy_table->append({static_cast<int64_t>(value)});
  }
  const auto memory_estimation = dummy_table->memory_usage(MemoryUsageCalculationMode::Full);

  // load measurement
  const auto system_utilization_load = generate_meta_table(meta_system_utilization_table);

  const auto virtual_memory_column_id = system_utilization_idle->column_id_by_name("process_virtual_memory");
  const auto virtual_memory_idle = boost::get<int64_t>(system_utilization_idle->get_row(0)[virtual_memory_column_id]);
  const auto virtual_memory_load = boost::get<int64_t>(system_utilization_load->get_row(0)[virtual_memory_column_id]);
  const auto virtual_memory_diff = virtual_memory_load - virtual_memory_idle;

  EXPECT_GE(static_cast<size_t>(virtual_memory_diff), memory_estimation);
}

}  // namespace opossum
