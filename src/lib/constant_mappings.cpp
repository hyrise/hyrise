#include "constant_mappings.hpp"

#include <unordered_map>

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/make_bimap.hpp"

namespace opossum {

const boost::bimap<AggregateFunction, std::string> aggregate_function_to_string =
    make_bimap<AggregateFunction, std::string>({
        {AggregateFunction::Min, "MIN"},
        {AggregateFunction::Max, "MAX"},
        {AggregateFunction::Sum, "SUM"},
        {AggregateFunction::Avg, "AVG"},
        {AggregateFunction::Count, "COUNT"},
        {AggregateFunction::CountDistinct, "COUNT DISTINCT"},
        {AggregateFunction::StandardDeviationSample, "STDDEV_SAMP"},
        {AggregateFunction::Any, "ANY"},
    });

const boost::bimap<FunctionType, std::string> function_type_to_string =
    make_bimap<FunctionType, std::string>({{FunctionType::Substring, "SUBSTR"}, {FunctionType::Concatenate, "CONCAT"}});

const boost::bimap<DataType, std::string> data_type_to_string =
    hana::fold(data_type_enum_string_pairs, boost::bimap<DataType, std::string>{}, [](auto map, auto pair) {
      map.insert({hana::first(pair), std::string{hana::second(pair)}});
      return map;
    });

const boost::bimap<EncodingType, std::string> encoding_type_to_string = make_bimap<EncodingType, std::string>({
    {EncodingType::Dictionary, "Dictionary"},
    {EncodingType::RunLength, "RunLength"},
    {EncodingType::FixedStringDictionary, "FixedStringDictionary"},
    {EncodingType::FrameOfReference, "FrameOfReference"},
    {EncodingType::LZ4, "LZ4"},
    {EncodingType::Unencoded, "Unencoded"},
});

const boost::bimap<FileType, std::string> file_type_to_string = make_bimap<FileType, std::string>(
    {{FileType::Tbl, "Tbl"}, {FileType::Csv, "Csv"}, {FileType::Binary, "Binary"}, {FileType::Auto, "Auto"}});

const boost::bimap<LogLevel, std::string> log_level_to_string = make_bimap<LogLevel, std::string>(
    {{LogLevel::Debug, "Debug"}, {LogLevel::Info, "Info"}, {LogLevel::Warning, "Warning"}});

const boost::bimap<VectorCompressionType, std::string> vector_compression_type_to_string =
    make_bimap<VectorCompressionType, std::string>({
        {VectorCompressionType::FixedWidthInteger, "Fixed-width integer"},
        {VectorCompressionType::BitPacking, "Bit-packing"},
    });

std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function) {
  return stream << aggregate_function_to_string.left.at(aggregate_function);
}

std::ostream& operator<<(std::ostream& stream, const FunctionType function_type) {
  return stream << function_type_to_string.left.at(function_type);
}

std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << data_type_to_string.left.at(data_type);
}

std::ostream& operator<<(std::ostream& stream, const EncodingType encoding_type) {
  return stream << encoding_type_to_string.left.at(encoding_type);
}

std::ostream& operator<<(std::ostream& stream, const FileType file_type) {
  return stream << file_type_to_string.left.at(file_type);
}

std::ostream& operator<<(std::ostream& stream, const LogLevel log_level) {
  return stream << log_level_to_string.left.at(log_level);
}

std::ostream& operator<<(std::ostream& stream, const VectorCompressionType vector_compression_type) {
  return stream << vector_compression_type_to_string.left.at(vector_compression_type);
}

std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type) {
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger4Byte: {
      stream << "FixedWidthInteger4Byte";
      break;
    }
    case CompressedVectorType::FixedWidthInteger2Byte: {
      stream << "FixedWidthInteger2Byte";
      break;
    }
    case CompressedVectorType::FixedWidthInteger1Byte: {
      stream << "FixedWidthInteger1Byte";
      break;
    }
    case CompressedVectorType::BitPacking: {
      stream << "BitPacking";
      break;
    }
    default:
      break;
  }
  return stream;
}

}  // namespace opossum
