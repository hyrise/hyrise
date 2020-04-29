#ifdef __linux__
#include <numa.h>
#endif

#include "base_test.hpp"

#include "utils/meta_tables/meta_system_information_table.hpp"

namespace opossum {

class MetaSystemInformationTest : public BaseTest {
 protected:
  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }
};

TEST_F(MetaSystemInformationTest, NumaInformation) {
  const auto meta_system_information_table = std::make_shared<MetaSystemInformationTable>();
  auto system_information = generate_meta_table(meta_system_information_table);

  const auto cpu_count_column_id = system_information->column_id_by_name("cpu_count");
  const auto numa_cpu_count_column_id = system_information->column_id_by_name("numa_cpu_count");

  const auto cpu_count = boost::get<int32_t>(system_information->get_row(0)[cpu_count_column_id]);
  const auto numa_cpu_count = boost::get<int32_t>(system_information->get_row(0)[numa_cpu_count_column_id]);

  EXPECT_GT(cpu_count, 0);
  EXPECT_GT(numa_cpu_count, 0);
  EXPECT_GE(cpu_count, numa_cpu_count);

#ifdef __linux__
  // Reduce useable CPUs by one, check if the change is reflected in the metatable and reset the CPU affinities.
  auto* default_cpu_mask = numa_allocate_cpumask();
  numa_sched_getaffinity(0, default_cpu_mask);

  auto* cpu_mask = numa_allocate_cpumask();
  copy_bitmask_to_bitmask(default_cpu_mask, cpu_mask);

  const uint32_t max_cpu = static_cast<uint32_t>(numa_num_configured_cpus());
  for (uint32_t cpu_index = 0; cpu_index < max_cpu; ++cpu_index) {
    if (numa_bitmask_isbitset(default_cpu_mask, cpu_index)) {
      cpu_mask = numa_bitmask_clearbit(cpu_mask, cpu_index);
      break;
    }
  }

  numa_sched_setaffinity(0, cpu_mask);
  numa_free_cpumask(cpu_mask);

  system_information = generate_meta_table(meta_system_information_table);
  const auto numa_cpu_count_altered = boost::get<int32_t>(system_information->get_row(0)[numa_cpu_count_column_id]);

  EXPECT_EQ(numa_cpu_count_altered, numa_cpu_count - 1);

  numa_sched_setaffinity(0, default_cpu_mask);
  numa_free_cpumask(default_cpu_mask);

  system_information = generate_meta_table(meta_system_information_table);
  const auto numa_cpu_count_reset = boost::get<int32_t>(system_information->get_row(0)[numa_cpu_count_column_id]);

  EXPECT_EQ(numa_cpu_count_reset, numa_cpu_count);
#endif
}

}  // namespace opossum
