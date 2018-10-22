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

namespace opossum {

class CalibrationQueryGeneratorTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(CalibrationQueryGeneratorTest, SimpleTest) {
  // set up some TableScanOperator

  std::map<std::string, CalibrationColumnSpecification> columns = {
      {"columnA", CalibrationColumnSpecification{"int", "uniform", false, 100, EncodingType::Unencoded}},
      {"columnB", CalibrationColumnSpecification{"int", "uniform", false, 100, EncodingType::Unencoded}},
      {"columnC", CalibrationColumnSpecification{"int", "uniform", false, 100, EncodingType::Unencoded}}};

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
