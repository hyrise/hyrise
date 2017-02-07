#include <chrono>
#include <iostream>
#include <locale>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "../lib/operators/get_table.hpp"
#include "../lib/operators/print.hpp"
#include "../lib/storage/storage_manager.hpp"

// #include "operators/join_hash.hpp"
#include "operators/sort_merge_join.hpp"

#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

void generate_distinct_values_for_table(std::shared_ptr<opossum::Table> table, size_t number_of_rows);
void generate_uniform_values_for_table(std::shared_ptr<opossum::Table> table, size_t number_of_rows,
                                       size_t highest_val);
void print_elapsed_time(std::string what_finished, std::chrono::time_point<std::chrono::high_resolution_clock> &start,
                        std::chrono::time_point<std::chrono::high_resolution_clock> &end, bool divider);
void compress_table(std::shared_ptr<opossum::Table> table);

int main(int argc, char *argv[]) {
  // Configuration
  size_t chunk_size = 1000;
  size_t number_of_rows = 10000000;
  size_t ratio = 16;

  if (argc >= 2) {
    chunk_size = atol(argv[1]);
  }
  if (argc >= 3) {
    number_of_rows = atol(argv[2]);
  }

  std::cout.imbue(std::locale(""));
  std::cout << "-----------------------------------------------" << std::endl;
  std::cout << "Starting Join Benchmark!" << std::endl;
  std::cout << "Chunk Size: " << chunk_size << std::endl;
  std::cout << "Table Sizes: " << number_of_rows << " x " << number_of_rows / ratio << std::endl;
  std::cout << "Distinct Values: " << number_of_rows << std::endl;
  std::cout << "-----------------------------------------------\n" << std::endl;

  auto start_time = std::chrono::high_resolution_clock::now();
  auto end_time = std::chrono::high_resolution_clock::now();

  // Build up table
  std::cout << "1. Initializing Tables" << std::endl;
  start_time = std::chrono::high_resolution_clock::now();
  std::shared_ptr<opossum::Table> left_table = std::make_shared<opossum::Table>(chunk_size);
  left_table->add_column("ColumnA", "int");
  left_table->add_column("ColumnB", "int");

  std::shared_ptr<opossum::Table> right_table = std::make_shared<opossum::Table>(chunk_size);
  right_table->add_column("ColumnA", "int");
  right_table->add_column("ColumnB", "int");

  // smaller primary key table
  generate_distinct_values_for_table(left_table, number_of_rows / ratio);
  // bigger foreign key table
  generate_uniform_values_for_table(right_table, number_of_rows, number_of_rows / ratio);

  opossum::StorageManager::get().add_table("table_left", std::move(left_table));
  opossum::StorageManager::get().add_table("table_right", std::move(right_table));

  auto _gt_left = std::make_shared<opossum::GetTable>("table_left");
  auto _gt_right = std::make_shared<opossum::GetTable>("table_right");
  _gt_left->execute();
  _gt_right->execute();

  end_time = std::chrono::high_resolution_clock::now();
  print_elapsed_time("Initialization", start_time, end_time, true);

  // Joining Two Value Columns
  auto colnames = std::make_pair(std::string("ColumnA"), std::string("ColumnA"));
  auto join_columns = opossum::optional<std::pair<const std::string &, const std::string &>>(colnames);

  std::cout << "Joining Tables with Value Columns" << std::endl;

  auto hash_join_vc =
      std::make_shared<opossum::SortMergeJoin>(_gt_left, _gt_right, join_columns, "=", opossum::JoinMode::Inner);

  start_time = std::chrono::high_resolution_clock::now();
  hash_join_vc->execute();
  end_time = std::chrono::high_resolution_clock::now();
  print_elapsed_time("Hash Join", start_time, end_time, false);

  // check output size
  auto output = hash_join_vc->get_output();
  if (number_of_rows != output->row_count()) {
    std::cout << "WARNING: Output size incorrect. It's " << output->row_count() << ", but should be "
              << (number_of_rows) << std::endl;
  }

  return 0;
}

void generate_uniform_values_for_table(std::shared_ptr<opossum::Table> table, size_t number_of_rows,
                                       size_t highest_val) {
  std::vector<int32_t> v;
  v.resize(number_of_rows);

  for (size_t i = 0; i < number_of_rows; ++i) {
    v[i] = i % highest_val;
  }

  std::random_shuffle(v.begin(), v.end());

  // append (two columns)
  for (const auto &val : v) {
    table->append({val, val});
  }
}

void generate_distinct_values_for_table(std::shared_ptr<opossum::Table> table, size_t number_of_rows) {
  std::vector<int32_t> v;
  v.resize(number_of_rows);

  // fill with values from 0 to max
  std::iota(v.begin(), v.end(), 0);

  // shuffle
  std::random_shuffle(v.begin(), v.end());

  // append (two columns)
  for (const auto &val : v) {
    table->append({val, val});
  }
}

void print_elapsed_time(std::string what_finished, std::chrono::time_point<std::chrono::high_resolution_clock> &start,
                        std::chrono::time_point<std::chrono::high_resolution_clock> &end, bool divider) {
  auto time = end - start;
  std::cout << "Finished " << what_finished << " after "
            << std::chrono::duration_cast<std::chrono::milliseconds>(time).count() << " ms" << std::endl;
  if (divider) {
    std::cout << "-------------------------------------------------" << std::endl;
  }
}

void compress_table(std::shared_ptr<opossum::Table> table) {
  for (opossum::ChunkID chunk_id = 0; chunk_id < table->chunk_count(); chunk_id++) {
    table->compress_chunk(chunk_id);
  }
}
