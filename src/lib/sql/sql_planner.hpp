#pragma once

#include <boost/noncopyable.hpp>

#include "SQLParserResult.h"

#include "operators/abstract_operator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

/**
 * This class wraps the translation of a parse result to an Operator tree.
 * This includes four steps:
 * - translate parse result to AST
 * - optimize AST
 * - translate AST to operators
 * - wrap operators in SQLQueryPlan
 */
class SQLPlanner final : public boost::noncopyable {
 public:
  static SQLQueryPlan plan(const hsql::SQLParserResult& result, bool validate = true);
};

}  // namespace opossum
