#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/termfactory.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class TermFactoryTest : public BaseTest {
 protected:
  void SetUp() override { _t = load_table("src/test/tables/int_string_like.tbl", 2); }

  std::shared_ptr<const Table> _t;
};

TEST_F(TermFactoryTest, EmptyExpression) {
  if (!IS_DEBUG) return;
  EXPECT_THROW(TermFactory<int>::build_term(""), std::logic_error);
}

TEST_F(TermFactoryTest, SimpelConstantExpression) {
  ASSERT_EQ(TermFactory<int32_t>::build_term("2")->get_values(_t, 0)[0], 2);
  ASSERT_EQ(TermFactory<int64_t>::build_term("2")->get_values(_t, 0)[0], 2l);
  ASSERT_EQ(TermFactory<double>::build_term("2.1")->get_values(_t, 0)[0], 2.1);
  ASSERT_EQ(TermFactory<float>::build_term("3.4")->get_values(_t, 0)[0], 3.4f);
}

TEST_F(TermFactoryTest, SimpelVariantExpression) {
  ASSERT_EQ(TermFactory<int32_t>::build_term("$a")->get_values(_t, 0)[0], 123);
}

TEST_F(TermFactoryTest, SimpelArithmeticConstantExpression) {
  ASSERT_EQ(TermFactory<int32_t>::build_term("1+2")->get_values(_t, 0)[0], 3);
}

TEST_F(TermFactoryTest, SimpelArithmeticMixedExpression) {
  ASSERT_EQ(TermFactory<int32_t>::build_term("1+$a")->get_values(_t, 0)[0], 124);
}

TEST_F(TermFactoryTest, ComplexArithmeticConstantExpression) {
  ASSERT_EQ(TermFactory<int32_t>::build_term("1+2*4/2%7-1")->get_values(_t, 0)[0], 4);
}

TEST_F(TermFactoryTest, ComplexArithmeticConstantExpressionTwo) {
  ASSERT_EQ(TermFactory<int32_t>::build_term("1+2*4-5")->get_values(_t, 0)[0], 4);
}

}  // namespace opossum
