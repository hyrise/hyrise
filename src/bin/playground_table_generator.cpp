#include <iostream>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "operators/export_binary.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "tpcc/tpcc_random_generator.hpp"
#include "tpcc/tpcc_table_generator.hpp"

class PlaygroundTableGenerator : public benchmark_utilities::AbstractBenchmarkTableGenerator {
 public:
  explicit PlaygroundTableGenerator(const opossum::ChunkOffset chunk_size = 10000, const size_t row_count = 100000)
      : AbstractBenchmarkTableGenerator(chunk_size), _row_count(row_count), _random_gen(tpcc::TpccRandomGenerator()) {}

  std::map<std::string, std::shared_ptr<opossum::Table>> generate_all_tables() override {
    auto cardinalities = std::make_shared<std::vector<size_t>>();
    cardinalities->emplace_back(_row_count);

    auto customer_table = std::make_shared<opossum::Table>(_chunk_size);

    add_column<int>(customer_table, "ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
    add_column<std::string>(customer_table, "NAME", cardinalities,
                            [&](std::vector<size_t> indices) { return _random_gen.last_name(indices[0]); });
    add_column<float>(customer_table, "BALANCE", cardinalities,
                      [&](std::vector<size_t>) { return _random_gen.random_number(-_row_count, _row_count) / 100.f; });
    add_column<float>(customer_table, "INTEREST", cardinalities,
                      [&](std::vector<size_t>) { return _random_gen.random_number(0, 1000) / 1000.f; });
    add_column<int>(customer_table, "LEVEL", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 5); });

    opossum::DictionaryCompression::compress_table(*customer_table);

    std::map<std::string, std::shared_ptr<opossum::Table>> tables;
    tables.insert(std::make_pair("CUSTOMER", customer_table));
    return tables;
  }

 private:
  size_t _row_count;
  tpcc::TpccRandomGenerator _random_gen;
};

int main() {
  std::cout << "Playground group 01 table generator" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  auto generator = PlaygroundTableGenerator{10'000, 10'000'000};
  auto tables = generator.generate_all_tables();

  for (auto& pair : tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
  }

  std::cout << " > Dumping as binary" << std::endl;
  auto customer_table = opossum::StorageManager::get().get_table("CUSTOMER");
  auto table_wrapper = std::make_shared<opossum::TableWrapper>(std::move(customer_table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, "group01_CUSTOMER.bin");
  ex->execute();

  std::cout << " > Done" << std::endl;

  return 0;
}
