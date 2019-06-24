#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "query/calibration_query_generator_projection.hpp"

namespace opossum {

class CalibrationQueryGeneratorProjectionTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(CalibrationQueryGeneratorProjectionTest, CheckColumns) {
  std::vector<std::pair<DataType, std::string>> column_definitions{};
  const std::vector<opossum::LQPColumnReference> columns = {LQPColumnReference(MockNode::make(column_definitions), ColumnID{0}),
                        LQPColumnReference(MockNode::make(column_definitions), ColumnID{1}),
                        LQPColumnReference(MockNode::make(column_definitions), ColumnID{2})};
  auto projection = CalibrationQueryGeneratorProjection::generate_projection(columns);

  ASSERT_TRUE(projection);
  EXPECT_LE(projection->column_expressions().size(), columns.size());
}

}  // namespace opossum
