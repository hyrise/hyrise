#include <string>
#include <unordered_map>

#include "sql/SelectStatement.h"
#include "types.hpp"
#include "utils/bimap.hpp"

namespace opossum {

enum class PredicateCondition;
enum class OrderByMode;
enum class JoinMode;
enum class UnionMode;
enum class EncodingType : uint8_t;
enum class FunctionType;
enum class VectorCompressionType : uint8_t;
enum class AggregateFunction;
enum class ExpressionType;
enum class TableType;

extern const Bimap<PredicateCondition, std::string> predicate_condition_to_string;
extern const std::unordered_map<OrderByMode, std::string> order_by_mode_to_string;
extern const std::unordered_map<hsql::OrderType, OrderByMode> order_type_to_order_by_mode;
extern const std::unordered_map<ExpressionType, std::string> expression_type_to_operator_string;
extern const std::unordered_map<JoinMode, std::string> join_mode_to_string;
extern const std::unordered_map<UnionMode, std::string> union_mode_to_string;
extern const Bimap<AggregateFunction, std::string> aggregate_function_to_string;
extern const Bimap<FunctionType, std::string> function_type_to_string;
extern const Bimap<DataType, std::string> data_type_to_string;
extern const Bimap<EncodingType, std::string> encoding_type_to_string;
extern const Bimap<VectorCompressionType, std::string> vector_compression_type_to_string;
extern const Bimap<TableType, std::string> table_type_to_string;

}  // namespace opossum
