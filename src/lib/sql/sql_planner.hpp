#pragma once

#include "SQLParserResult.h"

#include "operators/abstract_operator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

/**
 * This class wraps the translation of a parse result to an Operator tree.
 * This includes four steps:
 * - translate parse result to LQP
 * - optimize LQP
 * - translate LQP to operators
 * - wrap operators in SQLQueryPlan
 */
class SQLPlanner final : public Noncopyable {
 public:
  static SQLQueryPlan plan(const hsql::SQLParserResult& result, bool validate = true);
};

}  // namespace opossum
