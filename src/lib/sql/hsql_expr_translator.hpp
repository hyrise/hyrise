#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"

namespace opossum {

class AbstractExpression;
class SQLTranslationState;

std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& expr,
                                                        const std::shared_ptr<SQLTranslationState>& translation_state,
                                                        bool validate);

}  // namespace opossum
