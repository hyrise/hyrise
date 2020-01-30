#pragma once

#include <string>
#include <unordered_map>

#include <boost/bimap.hpp>
#include <operators/abstract_operator.hpp>

#include "sql/Expr.h"
#include "sql/SelectStatement.h"

#include "all_type_variant.hpp"
#include "expression/function_expression.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"
#include <synthetic_table_generator.hpp>
#include "storage/vector_compression/compressed_vector_type.hpp"

namespace opossum {

enum class EncodingType : uint8_t;
enum class VectorCompressionType : uint8_t;
enum class AggregateFunction;
enum class ExpressionType;

extern const boost::bimap<AggregateFunction, std::string> aggregate_function_to_string;
extern const boost::bimap<FunctionType, std::string> function_type_to_string;
extern const boost::bimap<DataType, std::string> data_type_to_string;
extern const boost::bimap<EncodingType, std::string> encoding_type_to_string;
extern const boost::bimap<VectorCompressionType, std::string> vector_compression_type_to_string;
extern const boost::bimap<DataDistributionType, std::string> data_distribution_type_to_string;
extern const boost::bimap<OperatorType, std::string> operator_type_to_string;
extern const boost::bimap<CompressedVectorType, std::string> compressed_vector_type_to_string;

std::ostream& operator<<(std::ostream& stream, AggregateFunction aggregate_function);
std::ostream& operator<<(std::ostream& stream, FunctionType function_type);
std::ostream& operator<<(std::ostream& stream, DataType data_type);
std::ostream& operator<<(std::ostream& stream, EncodingType encoding_type);
std::ostream& operator<<(std::ostream& stream, VectorCompressionType vector_compression_type);
std::ostream& operator<<(std::ostream& stream, OperatorType operator_type);
std::ostream& operator<<(std::ostream& stream, ColumnDataDistribution column_data_distribution);
std::ostream& operator<<(std::ostream& stream, CompressedVectorType compressed_vector_type);
std::ostream& operator<<(std::ostream& stream, const SegmentEncodingSpec& spec);

}  // namespace opossum
