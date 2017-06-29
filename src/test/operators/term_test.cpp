#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/term.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class TermTest : public BaseTest {
 protected:
  void SetUp() override { _t = load_table("src/test/tables/int_string_like.tbl", 2); }

  std::shared_ptr<const Table> _t;
};

TEST_F(TermTest, Constant) {
  ConstantTerm<std::string> term("b");

  // we don't actually need a table and position to evaluate a constant term
  // we are overriding the base function get_value and add the parameters for uniform handling of terms
  ASSERT_EQ("b", term.get_values(_t, ChunkID{0})[0]);
  ASSERT_EQ("b", term.get_value());
}

TEST_F(TermTest, Variable) {
  VariableTerm<std::string> term(ColumnName("b"));

  ASSERT_EQ("Schifffahrtsgesellschaft", type_cast<std::string>(term.get_values(_t, ChunkID{0})[0]));
  ASSERT_EQ("Dampfschifffahrtsgesellschaft", type_cast<std::string>(term.get_values(_t, ChunkID{0})[1]));
  ASSERT_EQ("Reeperbahn", type_cast<std::string>(term.get_values(_t, ChunkID{1})[0]));
}

TEST_F(TermTest, Arithmetic_GenericOperator) {
  std::shared_ptr<AbstractTerm<int>> variable_term(new VariableTerm<int>(ColumnName("a")));  // {1, 1} == 123456
  std::shared_ptr<AbstractTerm<int>> constant_term(new ConstantTerm<int>(2));

  {
    // PLUS
    ArithmeticTerm<int> arithmetic_term(variable_term, constant_term, "+");
    ASSERT_EQ(123458, arithmetic_term.get_values(_t, ChunkID{1})[1]);
  }

  {
    // MINUS
    ArithmeticTerm<int> arithmetic_term(variable_term, constant_term, "-");
    ASSERT_EQ(123454, arithmetic_term.get_values(_t, ChunkID{1})[1]);
  }

  {
    // MULTIPLY
    ArithmeticTerm<int> arithmetic_term(variable_term, constant_term, "*");
    ASSERT_EQ(246912, arithmetic_term.get_values(_t, ChunkID{1})[1]);
  }

  {
    // DIVIDE
    ArithmeticTerm<int> arithmetic_term(variable_term, constant_term, "/");
    ASSERT_EQ(61728, arithmetic_term.get_values(_t, ChunkID{1})[1]);
  }

  {
    // MODULO
    ArithmeticTerm<int> arithmetic_term(variable_term, constant_term, "%");
    ASSERT_EQ(0, arithmetic_term.get_values(_t, ChunkID{1})[1]);
  }
}

TEST_F(TermTest, Arithmetic_StringOperator) {
  std::shared_ptr<AbstractTerm<std::string>> variable_term(
      new VariableTerm<std::string>(ColumnName("b")));  // {1, 0} == Dampfschifffahrtsgesellschaftskapitän
  std::shared_ptr<AbstractTerm<std::string>> constant_term(new ConstantTerm<std::string>("test_string"));

  {
    // PLUS
    ArithmeticTerm<std::string> arithmetic_term(variable_term, constant_term, "+");
    ASSERT_EQ("Dampfschifffahrtsgesellschaftskapitäntest_string", arithmetic_term.get_values(_t, ChunkID{1})[1]);
  }

  if (IS_DEBUG) {
    {
      // Unimplemented
      EXPECT_THROW(ArithmeticTerm<std::string>(variable_term, constant_term, "-"), std::exception);
      EXPECT_THROW(ArithmeticTerm<std::string>(variable_term, constant_term, "*"), std::exception);
      EXPECT_THROW(ArithmeticTerm<std::string>(variable_term, constant_term, "/"), std::exception);
      EXPECT_THROW(ArithmeticTerm<std::string>(variable_term, constant_term, "%"), std::exception);
    }
  }
}

}  // namespace opossum
