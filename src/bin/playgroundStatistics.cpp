#include <iostream>
#include <memory>

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/table.hpp"

#include "../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "all_parameter_variant.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
std::vector<T> _split(const std::string &str, char delimiter) {
  std::vector<T> internal;
  std::stringstream ss(str);
  std::string tok;

  while (std::getline(ss, tok, delimiter)) {
    internal.push_back(tok);
  }

  return internal;
}

std::shared_ptr<Table> load_table(const std::string &file_name, size_t chunk_size) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file " + file_name);

  std::string line;
  std::getline(infile, line);
  std::vector<std::string> col_names = _split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> col_types = _split<std::string>(line, '|');

  auto col_nullable = std::vector<bool>{};
  for (auto &type : col_types) {
    auto type_nullable = _split<std::string>(type, '_');
    type = type_nullable[0];

    auto nullable = type_nullable.size() > 1 && type_nullable[1] == "null";
    col_nullable.push_back(nullable);
  }

  std::shared_ptr<Table> test_table = std::make_shared<Table>(chunk_size);
  for (size_t i = 0; i < col_names.size(); i++) {
    test_table->add_column(col_names[i], col_types[i], col_nullable[i]);
  }

  while (std::getline(infile, line)) {
    std::vector<AllTypeVariant> values = _split<AllTypeVariant>(line, '|');

    for (auto column_id = 0u; column_id < values.size(); ++column_id) {
      auto &value = values[column_id];
      auto nullable = col_nullable[column_id];

      if (nullable && (value == AllTypeVariant{"null"})) {
        value = NULL_VALUE;
      }
    }

    test_table->append(values);

    auto &chunk = test_table->get_chunk(static_cast<ChunkID>(test_table->chunk_count() - 1));
    auto mvcc_cols = chunk.mvcc_columns();
    mvcc_cols->begin_cids.back() = 0;
  }
  return test_table;
}

void test() {
  std::cout << "starting main" << std::endl;

  auto table = load_table("../src/test/tables/simple_int.tbl", 0);
  auto stats = std::make_shared<TableStatistics>(table);
  table->set_table_statistics(stats);

  //  cout << "Initial" << endl;
  //
  //  cout << *stats << endl;

  auto stats_w5 = stats->predicate_statistics(ColumnID(0), ScanType::OpLessThanEquals, AllTypeVariant(5));
  auto stats_w4 = stats->predicate_statistics(ColumnID(0), ScanType::OpLessThanEquals, AllTypeVariant(4));
  auto stats_w3 = stats->predicate_statistics(ColumnID(0), ScanType::OpLessThanEquals, AllTypeVariant(3));
  auto stats_w2 = stats->predicate_statistics(ColumnID(0), ScanType::OpLessThanEquals, AllTypeVariant(2));
  auto stats_w1 = stats->predicate_statistics(ColumnID(0), ScanType::OpLessThanEquals, AllTypeVariant(1));

  stats = stats_w1;

  std::cout << "simple scan w1" << std::endl;

  std::cout << *stats << std::endl << std::endl;

  stats =
      stats_w2->join_statistics(stats, std::make_pair(ColumnID(0), ColumnID(0)), ScanType::OpEquals, JoinMode::Outer);

  std::cout << "join with w2" << std::endl;

  std::cout << *stats << std::endl << std::endl;

  stats =
      stats_w3->join_statistics(stats, std::make_pair(ColumnID(0), ColumnID(0)), ScanType::OpEquals, JoinMode::Outer);

  std::cout << "join with w3" << std::endl;

  std::cout << *stats << std::endl << std::endl;

  stats =
      stats_w4->join_statistics(stats, std::make_pair(ColumnID(0), ColumnID(0)), ScanType::OpEquals, JoinMode::Outer);

  std::cout << "join with w4" << std::endl;

  std::cout << *stats << std::endl << std::endl;

  stats =
      stats_w5->join_statistics(stats, std::make_pair(ColumnID(0), ColumnID(0)), ScanType::OpEquals, JoinMode::Outer);

  std::cout << "join with w5" << std::endl;

  std::cout << *stats << std::endl << std::endl;

  //  tpcc::TpccTableGenerator generator;
  //
  //  std::cout << "starting generate table" << std::endl;
  //
  //  opossum::StorageManager::get().add_table("CUSTOMER", generator.generate_customer_table());
  //
  //  auto table_statistics = opossum::StorageManager::get().get_table("CUSTOMER")->table_statistics();
  //  auto stat1 = table_statistics->predicate_statistics(opossum::ColumnID(0), opossum::ScanType::OpEquals,
  //                                                      opossum::AllParameterVariant(1));  // "C_ID"
  //  auto stat2 = stat1->predicate_statistics(opossum::ColumnID(1), opossum::ScanType::OpNotEquals,
  //                                           opossum::AllParameterVariant(2));  // "C_D_ID"
  //  auto stat3 = stat1->predicate_statistics(opossum::ColumnID(1), opossum::ScanType::OpLessThan,
  //                                           opossum::AllParameterVariant(5));  // "C_D_ID"
  //  std::cout << "original CUSTOMER table" << std::endl;
  //  std::cout << *table_statistics << std::endl;
  //  std::cout << "C_ID = 1" << std::endl;
  //  std::cout << *stat1 << std::endl;
  //  std::cout << "C_D_ID != 2" << std::endl;
  //  std::cout << *stat2 << std::endl;
  //  std::cout << "C_D_ID < 5" << std::endl;
  //  std::cout << *stat3 << std::endl;
  //
  //  std::cout << "--- COLUMN Table Scans ---" << std::endl;
  //  stat1 = table_statistics->predicate_statistics(opossum::ColumnID(0), opossum::ScanType::OpEquals,
  //                                                 opossum::AllParameterVariant(opossum::ColumnID(1)));
  //  std::cout << "original CUSTOMER table" << std::endl;
  //  std::cout << *table_statistics << std::endl;
  //  std::cout << "C_ID = C_D_ID" << std::endl;
  //  std::cout << *stat1 << std::endl;
}

}  // namespace opossum

int main() { opossum::test(); }
