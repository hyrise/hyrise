#include <memory_resource>
#include <iostream>

#include "hyrise.hpp"
#include "types.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/segment_iterate.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT

int main() {
  // Timer t;
  // using String = std::string;
  // // using String = std::pmr::string;
  // auto x = String{};
  // auto y = String{"test"};
  // for (auto i = 0; i < 10000000; ++i) {
  //   x = String{String{y}};
  // }
  // std::cout << x << std::endl;
  // std::cout << t.lap_formatted() << std::endl;

  TPCHTableGenerator{.1f}.generate_and_store();

  const auto table = Hyrise::get().storage_manager.get_table("lineitem");
  // const auto table = SQLPipelineBuilder{"SELECT * FROM lineitem WHERE l_orderkey > 1"}.create_pipeline().get_result_table().second;

  std::cout << "Iterating over " << table->row_count() << " rows" << std::endl;

  Timer t;
  auto count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(ChunkID{0});
    const auto segment = chunk->get_segment(ColumnID{8});

    segment_with_iterators<pmr_string>(*segment, [&](auto it, const auto end) {
      while (it != end) {
        if (it->value() == "R") ++count;
        ++it;
      }
    });
  }
  std::cout << t.lap_formatted() << std::endl;
  std::cout << count << " matches" << std::endl;

  return 0;
}
