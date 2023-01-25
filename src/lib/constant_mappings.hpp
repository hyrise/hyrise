#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

enum class EncodingType : uint8_t;
enum class VectorCompressionType : uint8_t;
enum class AggregateFunction;
enum class FunctionType;
enum class CompressedVectorType : uint8_t;

std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function);
std::ostream& operator<<(std::ostream& stream, const FunctionType function_type);
std::ostream& operator<<(std::ostream& stream, const DataType data_type);
std::ostream& operator<<(std::ostream& stream, const EncodingType encoding_type);
std::ostream& operator<<(std::ostream& stream, const VectorCompressionType vector_compression_type);
std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type);

}  // namespace hyrise
