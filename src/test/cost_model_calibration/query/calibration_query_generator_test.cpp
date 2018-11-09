#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "configuration/calibration_column_specification.hpp"
#include "query/calibration_query_generator.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class CalibrationQueryGeneratorTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& manager = StorageManager::get();
    manager.add_table("SomeTable", load_table("src/test/tables/int_string.tbl", 1u));
  }
};

TEST_F(CalibrationQueryGeneratorTest, SimpleTest) {
  std::map<std::string, CalibrationColumnSpecification> columns = {
      {"a", CalibrationColumnSpecification{DataType::Int, "uniform", false, 100, EncodingType::Unencoded}},
      {"b", CalibrationColumnSpecification{DataType::String, "uniform", false, 100, EncodingType::Unencoded}}};

  std::vector<CalibrationTableSpecification> tables = {
      CalibrationTableSpecification{"SomePath", "SomeTable", 1000, columns}};

  for (int i = 0; i < 10; i++) {
    auto queries = CalibrationQueryGenerator::generate_queries(tables);

    for (const auto& query : queries) {
      std::cout << query << std::endl;
    }
  }
}

}  // namespace opossum
