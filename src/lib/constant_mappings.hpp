#pragma once

#include <string>
#include <unordered_map>

#include <boost/bimap.hpp>

#include "sql/Expr.h"
#include "sql/SelectStatement.h"

#include "all_type_variant.hpp"
#include "expression/function_expression.hpp"
#include "import_export/file_type.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {

enum class EncodingType : uint8_t;
enum class VectorCompressionType : uint8_t;
enum class AggregateFunction;
enum class ExpressionType;
enum class FileType;

extern const boost::bimap<AggregateFunction, std::string> aggregate_function_to_string;
extern const boost::bimap<FunctionType, std::string> function_type_to_string;
extern const boost::bimap<DataType, std::string> data_type_to_string;
extern const boost::bimap<EncodingType, std::string> encoding_type_to_string;
extern const boost::bimap<FileType, std::string> file_type_to_string;
extern const boost::bimap<VectorCompressionType, std::string> vector_compression_type_to_string;

std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function);
std::ostream& operator<<(std::ostream& stream, const FunctionType function_type);
std::ostream& operator<<(std::ostream& stream, const DataType data_type);
std::ostream& operator<<(std::ostream& stream, const EncodingType encoding_type);
std::ostream& operator<<(std::ostream& stream, const FileType file_type);
std::ostream& operator<<(std::ostream& stream, const VectorCompressionType vector_compression_type);
std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type);
std::ostream& operator<<(std::ostream& stream, const SegmentEncodingSpec& spec);

}  // namespace opossum
