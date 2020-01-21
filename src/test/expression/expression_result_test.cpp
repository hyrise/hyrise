#include "base_test.hpp"

#include "expression/evaluation/expression_result.hpp"
#include "expression/evaluation/expression_result_views.hpp"

namespace opossum {

class ExpressionResultTest : public BaseTest {
 public:
  template <typename ExpectedViewType>
  bool check_view(std::vector<typename ExpectedViewType::Type> values, std::vector<bool> nulls) {
    auto match = false;
    ExpressionResult<typename ExpectedViewType::Type>(values, nulls).as_view([&](const auto& view) {
      match = std::is_same_v<std::decay_t<decltype(view)>, ExpectedViewType>;
    });
    return match;
  }
};

TEST_F(ExpressionResultTest, AsView) {
  EXPECT_TRUE(check_view<ExpressionResultLiteral<int32_t>>({5}, {false}));
  EXPECT_TRUE(check_view<ExpressionResultLiteral<int32_t>>({5}, {}));
  EXPECT_TRUE(check_view<ExpressionResultLiteral<int32_t>>({5}, {true}));
  EXPECT_TRUE(check_view<ExpressionResultNullableSeries<int32_t>>({5, 6, 7}, {false, true, false}));
  EXPECT_TRUE(check_view<ExpressionResultNonNullSeries<int32_t>>({5, 6, 7}, {}));
}

TEST_F(ExpressionResultTest, DataAccess) {
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {}).is_null(0), false);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {}).is_null(10), false);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {}).value(0), 5);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {}).value(10), 5);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {false}).is_null(0), false);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {false}).is_null(10), false);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {false}).value(0), 5);
  EXPECT_EQ(ExpressionResult<int32_t>({5}, {false}).value(10), 5);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {}).is_null(0), false);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {}).is_null(1), false);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {}).value(0), 3);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {}).value(1), 5);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {true, false}).is_null(0), true);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {true, false}).is_null(1), false);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {true, false}).value(0), 3);
  EXPECT_EQ(ExpressionResult<int32_t>({3, 5}, {true, false}).value(1), 5);
}

}  // namespace opossum
