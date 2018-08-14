#pragma once

#include <boost/bimap.hpp>
#include <string>
#include <unordered_map>

namespace opossum {

enum class JitExpressionType;

extern const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string;

}  // namespace opossum
