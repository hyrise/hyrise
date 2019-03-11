#include <iostream>

#include <chrono>

#include "types.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "benchmark_config.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;  // NOLINT

int main() {
  const auto benchmark_config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());

  for (const auto block_size : {500u, 1'000u, 4'000u, 4500u, 5'000u, 6'000u, 7'000u, 8'000u, 9'000U, 10'000u, 50'000u, 100'000u, 1'000'000u, 2'000'000u}) {
    benchmark_config->chunk_size = block_size;
    TpchTableGenerator{0.5f, benchmark_config}.generate_and_store();

    const auto lineitem = StorageManager::get().get_table("lineitem");
    const auto l_extendedprice = pqp_column_(ColumnID{5}, DataType::Float, false, "l_extendedprice");
    const auto l_discount = pqp_column_(ColumnID{6}, DataType::Float, false, "l_discount");
    const auto l_tax = pqp_column_(ColumnID{7}, DataType::Float, false, "l_tax");

    ExpressionEvaluator evaluator(lineitem, ChunkID{0});

    const auto begin = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    auto num_rows_processed = size_t{0};

    while (std::chrono::duration_cast<std::chrono::seconds>(end - begin) < std::chrono::seconds(5)) {
      const auto result = evaluator.evaluate_expression_to_segment(*mul_(mul_(l_extendedprice, sub_(1.0f, l_discount)), add_(1.0f, l_tax)));
      num_rows_processed += result->size();

      end = std::chrono::high_resolution_clock::now();
    }

    const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

    std::cout << "BlockSize: " << lineitem->get_chunk(ChunkID{0})->size() << ": " << num_rows_processed << " rows processed in " << elapsed_us << "us. ";
    std::cout << (num_rows_processed ? num_rows_processed/elapsed_us : 0) << " rows per us" << std::endl;

    StorageManager::reset();
  }

  return 0;
}
