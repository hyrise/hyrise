#ifdef __linux__
#include <sched.h>
#endif

#include "base_test.hpp"

#include "utils/meta_tables/meta_system_utilization_table.hpp"

namespace opossum {

class MetaSystemUtilizationTest : public BaseTest {
 protected:
  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }
};

#ifdef __linux__
// Reduce useable CPUs by one, check if the change is reflected in the metatable and reset the CPU affinities.
TEST_F(MetaSystemUtilizationTest, CPUAffinity) {
  const auto meta_system_utilization_table = std::make_shared<MetaSystemUtilizationTable>();
  auto system_utilization = generate_meta_table(meta_system_utilization_table);

  const auto cpu_affinity_count_column_id = system_utilization->column_id_by_name("cpu_affinity_count");
  const auto cpu_affinity_count = boost::get<int32_t>(system_utilization->get_row(0)[cpu_affinity_count_column_id]);

  cpu_set_t default_cpu_set;
  CPU_ZERO(&default_cpu_set);
  pthread_getaffinity_np(pthread_self(), sizeof(default_cpu_set), &default_cpu_set);

  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  memcpy(&cpu_set, &default_cpu_set, sizeof(cpu_set));

  for (uint32_t cpu_index = 0; cpu_index < CPU_SETSIZE; ++cpu_index) {
    if (CPU_ISSET(cpu_index, &default_cpu_set)) {
      CPU_CLR(cpu_index, &cpu_set);
      break;
    }
  }

  // Altering the CPU set of this worker thread should not alter the number on cpus for hyrise
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
  system_utilization = generate_meta_table(meta_system_utilization_table);
  const auto cpu_affinity_count_altered =
      boost::get<int32_t>(system_utilization->get_row(0)[cpu_affinity_count_column_id]);

  EXPECT_EQ(cpu_affinity_count_altered, cpu_affinity_count);

  pthread_setaffinity_np(pthread_self(), sizeof(default_cpu_set), &default_cpu_set);

  system_utilization = generate_meta_table(meta_system_utilization_table);
  const auto cpu_affinity_count_reset =
      boost::get<int32_t>(system_utilization->get_row(0)[cpu_affinity_count_column_id]);

  EXPECT_EQ(cpu_affinity_count_reset, cpu_affinity_count);
}
#endif

}  // namespace opossum
