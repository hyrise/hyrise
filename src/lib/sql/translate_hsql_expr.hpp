#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class SQLIdentifierContext;

std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& expr,
                                                        const std::shared_ptr<SQLIdentifierContext>& sql_identifier_context,
                                                        const UseMvcc use_mvcc);
}  // namespace opossum
