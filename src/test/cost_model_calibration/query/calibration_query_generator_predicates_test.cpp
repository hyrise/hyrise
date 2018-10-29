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
#include "query/calibration_query_generator_predicates.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class CalibrationQueryGeneratorPredicatesTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& manager = StorageManager::get();
    manager.add_table("SomeTable", load_table("src/test/tables/int_string_filtered.tbl", 1u));
  }
};

TEST_F(CalibrationQueryGeneratorPredicatesTest, SimpleTest) {
  std::map<std::string, CalibrationColumnSpecification> columns = {
      {"a", CalibrationColumnSpecification{"int", "uniform", false, 100, EncodingType::Unencoded}},
      {"b", CalibrationColumnSpecification{"string", "uniform", false, 100, EncodingType::Unencoded}}};

  CalibrationTableSpecification table_definition{"SomePath", "SomeTable", 1000, columns};

  const auto filter_column = table_definition.columns.find("b");
  auto predicate =
      CalibrationQueryGeneratorPredicates::generate_equi_predicate_for_strings(*filter_column, table_definition, "");

  EXPECT_TRUE(predicate);
  EXPECT_EQ(*predicate, "b = 'A'");
}

}  // namespace opossum
