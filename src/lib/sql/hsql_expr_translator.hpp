#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "all_parameter_variant.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class AbstractExpression;

std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& expr,
                                                        const std::vector<std::shared_ptr<AbstractLQPNode>>& nodes);

}  // namespace opossum
