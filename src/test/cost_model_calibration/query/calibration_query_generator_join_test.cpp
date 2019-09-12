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
    auto& manager = Hyrise::get().storage_manager;
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_string_float_calibration.tbl", 1u));
  }
};
/*
TEST_F(CalibrationQueryGeneratorJoinTest, GenerateJoinPredicate) {
  // TODO(Sven): Simplify interface!
  const std::vector<std::pair<DataType, std::string>> columns = {{DataType::Int, "a"}, {DataType::Int, "column_pk"}};
  const std::vector<CalibrationColumnSpecification> column_definitions{
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_pk", DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1}};
  const auto left_input = StoredTableNode::make("SomeTable");
  const auto right_input = StoredTableNode::make("SomeTable");

  CalibrationQueryGeneratorJoinConfiguration join_configuration{"SomeTable", "SomeTable", EncodingType::Unencoded,
                                                                DataType::Int, false};
  auto join_predicate = CalibrationQueryGeneratorJoin::generate_join_predicate(join_configuration, left_input,
                                                                               right_input, column_definitions);

  ASSERT_TRUE(join_predicate);
  ASSERT_EQ("a = column_pk", join_predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorJoinTest, GenerateJoinPermutation) {
  const std::vector<std::pair<std::string, size_t>> tables = {{"SomeTable", 100}, {"SomeOtherTable", 1000}};

  CalibrationConfiguration configuration{};
  configuration.encodings = {EncodingType::Dictionary};
  configuration.data_types = {DataType::Int};

  const auto result = CalibrationQueryGeneratorJoin::generate_join_permutations(tables, configuration);

  std::cout << result.size() << std::endl;
  for (const auto& f : result) {
    std::cout << f.left_table_name << std::endl;
    std::cout << f.right_table_name << std::endl;
    //    std::cout << f.encoding_type << std::endl;
    //    std::cout << f.data_type << std::endl;
    std::cout << f.reference_column << std::endl;
  }
}
*/
}  // namespace opossum
