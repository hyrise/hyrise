#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "operators/export_binary.hpp"
#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/deprecated_dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

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

    std::random_device rd;
    std::mt19937 e2(rd());
    // gaussian normal distribution for customer "VIP" level
    std::normal_distribution<> level_dist(3, 2);  // level 1-5 where most customers have level 3

    add_column<int>(customer_table, "ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

    auto first_names = read_vector_from_file("src/test/names/firstNames.txt");
    auto last_names = read_vector_from_file("src/test/names/lastNames.txt");

    add_column<std::string>(customer_table, "NAME", cardinalities, [&](std::vector<size_t> indices) {
      // return _random_gen.generate_string(5, 10, 'a', 26) + " " + _random_gen.generate_string(5, 10, 'a', 26);
      auto first_name = (*first_names)[_random_gen.random_number(0, first_names->size())];
      auto last_name = (*last_names)[_random_gen.random_number(0, last_names->size())];
      return first_name + " " + last_name;
    });
    add_column<int>(customer_table, "BALANCE", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(-_row_count, _row_count); });
    add_column<float>(customer_table, "INTEREST", cardinalities,
                      [&](std::vector<size_t>) { return _random_gen.random_number(0, 1000) / 1000.f; });
    add_column<int>(customer_table, "LEVEL", cardinalities, [&](std::vector<size_t>) {
      return std::max(1, std::min(5, static_cast<int>(std::round(level_dist(e2)))));
    });

    opossum::DeprecatedDictionaryCompression::compress_table(*customer_table);

    std::map<std::string, std::shared_ptr<opossum::Table>> tables;
    tables.insert(std::make_pair("CUSTOMER", customer_table));
    return tables;
  }

  std::shared_ptr<std::vector<std::string>> read_vector_from_file(std::string filename) {
    std::string line;
    auto output = std::make_shared<std::vector<std::string>>();
    std::ifstream inputfile(filename);
    opossum::Assert(inputfile, "Cannot open file " + filename);

    while (std::getline(inputfile, line)) {
      output->push_back(line);
    }

    return output;
  }

 private:
  size_t _row_count;
  tpcc::TpccRandomGenerator _random_gen;
};

int main() {
  std::cout << "Playground group 01 table generator" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  auto generator = PlaygroundTableGenerator{100'000, 1'000'000};
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

  std::cout << " > Dumping as CSV" << std::endl;
  auto csv_ex = std::make_shared<opossum::ExportCsv>(table_wrapper, "group01_CUSTOMER.csv");
  csv_ex->execute();

  std::cout << " > Done" << std::endl;

  return 0;
}
