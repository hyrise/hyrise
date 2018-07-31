#include <boost/bimap.hpp>
#include <string>
#include <unordered_map>

#include "sql/Expr.h"
#include "sql/SelectStatement.h"

#include "all_type_variant.hpp"
#include "expression/function_expression.hpp"
#include "operators/jit_operator/jit_types.hpp"
#include "types.hpp"

namespace opossum {

enum class EncodingType : uint8_t;
enum class VectorCompressionType : uint8_t;
enum class AggregateFunction;
enum class ExpressionType;
enum class TableType;

extern const boost::bimap<PredicateCondition, std::string> predicate_condition_to_string;
extern const std::unordered_map<OrderByMode, std::string> order_by_mode_to_string;
extern const std::unordered_map<hsql::OrderType, OrderByMode> order_type_to_order_by_mode;
extern const std::unordered_map<ExpressionType, std::string> expression_type_to_operator_string;
extern const std::unordered_map<JoinMode, std::string> join_mode_to_string;
extern const std::unordered_map<UnionMode, std::string> union_mode_to_string;
extern const boost::bimap<AggregateFunction, std::string> aggregate_function_to_string;
extern const boost::bimap<FunctionType, std::string> function_type_to_string;
extern const boost::bimap<DataType, std::string> data_type_to_string;
extern const boost::bimap<EncodingType, std::string> encoding_type_to_string;
extern const boost::bimap<VectorCompressionType, std::string> vector_compression_type_to_string;
extern const boost::bimap<TableType, std::string> table_type_to_string;
extern const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string;

}  // namespace opossum
