#ifdef __linux__
#include <sched.h>
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
  const auto cpu_affinity_count_column_id = system_information->column_id_by_name("cpu_affinity_count");

  const auto cpu_count = boost::get<int32_t>(system_information->get_row(0)[cpu_count_column_id]);
  const auto cpu_affinity_count = boost::get<int32_t>(system_information->get_row(0)[cpu_affinity_count_column_id]);

  EXPECT_GT(cpu_count, 0);
  EXPECT_GT(cpu_affinity_count, 0);
  EXPECT_GE(cpu_count, cpu_affinity_count);

#ifdef __linux__
  // Reduce useable CPUs by one, check if the change is reflected in the metatable and reset the CPU affinities.
  auto* default_cpu_set = CPU_ALLOC(cpu_count);
  const auto cpu_set_size = CPU_ALLOC_SIZE(cpu_count);
  sched_getaffinity(0, cpu_set_size, default_cpu_set);

  auto* cpu_set = CPU_ALLOC(cpu_count);
  memcpy(cpu_set, default_cpu_set, cpu_set_size);

  const auto max_cpu = static_cast<uint32_t>(cpu_count);
  for (uint32_t cpu_index = 0; cpu_index < max_cpu; ++cpu_index) {
    if (CPU_ISSET(cpu_index, default_cpu_set)) {
      CPU_CLR(cpu_index, cpu_set);
      break;
    }
  }

  sched_setaffinity(0, cpu_set_size, cpu_set);
  CPU_FREE(cpu_set);

  system_information = generate_meta_table(meta_system_information_table);
  const auto cpu_affinity_count_altered =
      boost::get<int32_t>(system_information->get_row(0)[cpu_affinity_count_column_id]);

  EXPECT_EQ(cpu_affinity_count_altered, cpu_affinity_count - 1);

  sched_setaffinity(0, cpu_set_size, default_cpu_set);
  CPU_FREE(default_cpu_set);

  system_information = generate_meta_table(meta_system_information_table);
  const auto cpu_affinity_count_reset =
      boost::get<int32_t>(system_information->get_row(0)[cpu_affinity_count_column_id]);

  EXPECT_EQ(cpu_affinity_count_reset, cpu_affinity_count);
#endif
}

}  // namespace opossum
