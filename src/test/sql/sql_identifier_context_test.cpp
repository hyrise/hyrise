#include "gtest/gtest.h"

#include <memory>

#include "expression/abstract_expression.hpp"
#include "expression/sql_identifier_expression.hpp"
#include "sql/sql_identifier_context.hpp"

namespace opossum {

class SQLIdentifierContextTest : public ::testing::Test {
 public:
  void SetUp() override {
    _expression_a = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"a", "T1"});
    _expression_b = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"b", "T1"});
    _expression_c = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"c", "T1"});
  }

 private:
  std::shared_ptr<AbstractExpression> _expression_a, _expression_b, _expression_c;
};

TEST_F(SQLIdentifierContextTest, Basics) {

}

}  // namespace opossum
