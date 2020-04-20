#include "base_test.hpp"

#include "operators/table_wrapper.hpp"
#include "utils/meta_tables/meta_system_utilization_table.hpp"

namespace opossum {

class MetaSystemUtilizationTest : public BaseTest {
 protected:
  std::shared_ptr<AbstractMetaTable> meta_system_utilization_table;
  
  void SetUp() {
    meta_system_utilization_table = std::make_shared<MetaSystemUtilizationTable>();
  }

  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }
};

TEST_F(MetaSystemUtilizationTest, ProcessMemoryUsage) {
	const auto system_utilization_reference = generate_meta_table(meta_system_utilization_table);

	const auto virtual_memory_column_id = system_utilization_reference->column_id_by_name("process_virtual_memory");
	const auto rss_column_id = system_utilization_reference->column_id_by_name("process_physical_memory_RSS");

	const auto dummy_table = std::make_shared<Table>(TableColumnDefinitions{{"dummy_column", DataType::Long, false}},
                                                     TableType::Data, 5);
	// Creates at least 625000 * 64 Bit = 1MB
	const auto memory_lower_bound = 1024 * 1024; // 1MB
	const auto dummy_table_columns = memory_lower_bound / 64;
	for (int column = 0; column < dummy_table_columns; ++column) {
		dummy_table->append({static_cast<int64_t>(column)});	
	}

	const auto system_utilization_load = generate_meta_table(meta_system_utilization_table);

	const auto virtual_memory_reference = system_utilization_reference->get_row(0)[virtual_memory_column_id];
	const auto virtual_memory_load = system_utilization_load->get_row(0)[virtual_memory_column_id];
	const auto virtual_memory_diff = boost::get<int64_t>(virtual_memory_load) - boost::get<int64_t>(virtual_memory_reference);
 
	const auto rss_reference = system_utilization_reference->get_row(0)[rss_column_id];
	const auto rss_load = system_utilization_load->get_row(0)[rss_column_id];
	const auto rss_diff = boost::get<int64_t>(rss_load) - boost::get<int64_t>(rss_reference);

	EXPECT_TRUE(virtual_memory_diff >= memory_lower_bound);
  	EXPECT_TRUE(rss_diff >= memory_lower_bound);
}


// This test may fail if it is executed in parallel with oth memory intensive processes.
TEST_F(MetaSystemUtilizationTest, SystemMemoryUsage) {
	const auto system_utilization_reference = generate_meta_table(meta_system_utilization_table);

	const auto free_memory_column_id = system_utilization_reference->column_id_by_name("system_memory_free");
	const auto available_memory_column_id = system_utilization_reference->column_id_by_name("system_memory_available");

	const auto dummy_table = std::make_shared<Table>(TableColumnDefinitions{{"dummy_column", DataType::Long, false}},
                                                     TableType::Data, 5);
	// Creates at least 625000 * 64 Bit = 1MB
	const auto memory_lower_bound = 1024 * 1024; // 1MB
	const auto dummy_table_columns = memory_lower_bound / 64;
	for (int column = 0; column < dummy_table_columns; ++column) {
		dummy_table->append({static_cast<int64_t>(column)});	
	}

	const auto system_utilization_load = generate_meta_table(meta_system_utilization_table);

	const auto free_memory_reference = system_utilization_reference->get_row(0)[free_memory_column_id];
	const auto free_memory_load = system_utilization_load->get_row(0)[free_memory_column_id];
	const auto free_memory_diff = boost::get<int64_t>(free_memory_reference) - boost::get<int64_t>(free_memory_load);
 
 	const auto available_memory_reference = system_utilization_reference->get_row(0)[available_memory_column_id];
 	const auto available_memory_load = system_utilization_load->get_row(0)[available_memory_column_id];
	const auto available_memory_diff = boost::get<int64_t>(available_memory_reference) - boost::get<int64_t>(available_memory_load);

  	EXPECT_TRUE(free_memory_diff >= memory_lower_bound);
  	EXPECT_TRUE(available_memory_diff >= memory_lower_bound);
}

TEST_F(MetaSystemUtilizationTest, ProcessCPUUsage) {

}

}  // namespace opossum
