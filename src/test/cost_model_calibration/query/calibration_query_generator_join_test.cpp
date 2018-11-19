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

#include "logical_query_plan/mock_node.hpp"
#include "query/calibration_query_generator_join.hpp"

namespace opossum {

class CalibrationQueryGeneratorJoinTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& manager = StorageManager::get();
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_calibration.tbl", 1u));
  }
};

TEST_F(CalibrationQueryGeneratorJoinTest, GenerateJoinPredicate) {
  // TODO(Sven): Simplify interface!
  const std::vector<std::pair<DataType, std::string>> columns = {{DataType::Int, "a"}, {DataType::Int, "column_pk"}};
  const std::vector<CalibrationColumnSpecification> column_definitions{
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"column_pk", DataType::Int, "uniform", false, 2, EncodingType::Unencoded}};
  const auto left_input = StoredTableNode::make("SomeTable");
  const auto right_input = StoredTableNode::make("SomeTable");

  CalibrationQueryGeneratorJoinConfiguration join_configuration{"SomeTable", "SomeTable", EncodingType::Unencoded, DataType::Int, false};
  auto join_predicate = CalibrationQueryGeneratorJoin::generate_join_predicate(join_configuration, left_input,
                                                                               right_input, column_definitions);

  ASSERT_TRUE(join_predicate);
  ASSERT_EQ("a = column_pk", join_predicate->as_column_name());
}

}  // namespace opossum
