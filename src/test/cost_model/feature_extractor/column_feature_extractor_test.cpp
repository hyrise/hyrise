#include <unordered_map>

#include "gtest/gtest.h"

#include "cost_estimation/feature_extractor/column_feature_extractor.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DISABLED_ColumnFeatureExtractorTest : public ::testing::Test {
 public:
  void SetUp() override {}
};

TEST_F(DISABLED_ColumnFeatureExtractorTest, UnencodedColumn) {
  const auto node = MockNode::make(MockNode::ColumnDefinitions{
      {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}});

  const auto column_expression = lqp_column_(node, ColumnID{0});
  const auto column_features = cost_model::ColumnFeatureExtractor::extract_features(node, column_expression, "");

  // TODO(anyone): refactoring removed those members. Update.
  // EXPECT_EQ(column_features.column_is_reference_segment, false);
  // EXPECT_EQ(column_features.column_data_type, DataType::Int);
  // EXPECT_EQ(column_features.column_segment_encoding_Dictionary_percentage, 0.0);
  // EXPECT_EQ(column_features.column_segment_encoding_FixedStringDictionary_percentage, 0.0);
  // EXPECT_EQ(column_features.column_segment_encoding_FrameOfReference_percentage, 0.0);
  // EXPECT_EQ(column_features.column_segment_encoding_RunLength_percentage, 0.0);
  // EXPECT_EQ(column_features.column_segment_encoding_Unencoded_percentage, 1.0);
}

}  // namespace opossum
