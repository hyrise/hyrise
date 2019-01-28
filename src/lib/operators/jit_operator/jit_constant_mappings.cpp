#include "jit_constant_mappings.hpp"

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>

#include <string>
#include <unordered_map>

#include "jit_types.hpp"
#include "utils/make_bimap.hpp"

namespace opossum {

// clang-tidy does not like global non-trivial objects that are not initialized with constexpr
// NOLINTNEXTLINE(fuchsia-statically-constructed-objects)
const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string =
    make_bimap<JitExpressionType, std::string>({{JitExpressionType::Addition, "+"},
                                                {JitExpressionType::Column, "<COLUMN>"},
                                                {JitExpressionType::Subtraction, "-"},
                                                {JitExpressionType::Multiplication, "*"},
                                                {JitExpressionType::Division, "/"},
                                                {JitExpressionType::Modulo, "%"},
                                                {JitExpressionType::Power, "^"},
                                                {JitExpressionType::Equals, "="},
                                                {JitExpressionType::NotEquals, "<>"},
                                                {JitExpressionType::GreaterThan, ">"},
                                                {JitExpressionType::GreaterThanEquals, ">="},
                                                {JitExpressionType::LessThan, "<"},
                                                {JitExpressionType::LessThanEquals, "<="},
                                                {JitExpressionType::Like, "LIKE"},
                                                {JitExpressionType::NotLike, "NOT LIKE"},
                                                {JitExpressionType::And, "AND"},
                                                {JitExpressionType::Or, "OR"},
                                                {JitExpressionType::Not, "NOT"},
                                                {JitExpressionType::IsNull, "IS NULL"},
                                                {JitExpressionType::IsNotNull, "IS NOT NULL"}});

}  // namespace opossum
