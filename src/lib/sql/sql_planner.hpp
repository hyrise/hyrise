#pragma once

#include <boost/noncopyable.hpp>

#include "SQLParserResult.h"
#include "operators/abstract_operator.hpp"
#include "sql_query_plan.hpp"

namespace opossum {

class SQLPlanner final : public boost::noncopyable {
 public:
  static SQLQueryPlan plan(const hsql::SQLParserResult& result);
};

}  // namespace opossum
