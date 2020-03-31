#include "constant_mappings.hpp"

#include <unordered_map>

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>
#include <operators/abstract_operator.hpp>

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

const boost::bimap<VectorCompressionType, std::string> vector_compression_type_to_string =
    make_bimap<VectorCompressionType, std::string>({
        {VectorCompressionType::FixedSizeByteAligned, "Fixed-size byte-aligned"},
        {VectorCompressionType::SimdBp128, "SIMD-BP128"},
    });

const boost::bimap<DataDistributionType, std::string> data_distribution_type_to_string =
    make_bimap<DataDistributionType, std::string>({
        {DataDistributionType::Uniform, "Uniform"},
        {DataDistributionType::NormalSkewed, "NormalSkewed"},
        {DataDistributionType::Pareto, "Pareto"},
    });

const boost::bimap<OperatorType, std::string> operator_type_to_string = make_bimap<OperatorType, std::string>({
    {OperatorType::Aggregate, "Aggregate"},
    {OperatorType::Alias, "Alias"},
    {OperatorType::Delete, "Delete"},
    {OperatorType::Difference, "Difference"},
    {OperatorType::Export, "Export"},
    {OperatorType::GetTable, "GetTable"},
    {OperatorType::Import, "Import"},
    {OperatorType::IndexScan, "IndexScan"},
    {OperatorType::Insert, "Insert"},
    {OperatorType::JoinHash, "JoinHash"},
    {OperatorType::JoinIndex, "JoinIndex"},
    {OperatorType::JoinNestedLoop, "JoinNestedLoop"},
    {OperatorType::JoinSortMerge, "JoinSortMerge"},
    {OperatorType::JoinVerification, "JoinVerification"},
    {OperatorType::Limit, "Limit"},
    {OperatorType::Print, "Print"},
    {OperatorType::Product, "Product"},
    {OperatorType::Projection, "Projection"},
    {OperatorType::Sort, "Sort"},
    {OperatorType::TableScan, "TableScan"},
    {OperatorType::TableWrapper, "TableWrapper"},
    {OperatorType::UnionAll, "UnionAll"},
    {OperatorType::UnionPositions, "UnionPositions"},
    {OperatorType::Update, "Update"},
    {OperatorType::Validate, "Validate"},
    {OperatorType::CreateTable, "CreateTable"},
    {OperatorType::CreatePreparedPlan, "CreatePreparedPlan"},
    {OperatorType::CreateView, "CreateView"},
    {OperatorType::DropTable, "DropTable"},
    {OperatorType::DropView, "DropView"},
    {OperatorType::Mock, "Mock"},
});

const boost::bimap<CompressedVectorType, std::string> compressed_vector_type_to_string =
    make_bimap<CompressedVectorType, std::string>({
        {CompressedVectorType::FixedSize4ByteAligned, "FixedSize4ByteAligned"},
        {CompressedVectorType::FixedSize2ByteAligned, "FixedSize2ByteAligned"},
        {CompressedVectorType::FixedSize1ByteAligned, "FixedSize1ByteAligned"},
        {CompressedVectorType::SimdBp128, "SimdBp128"},
    });

std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function) {
  return stream << aggregate_function_to_string.left.at(aggregate_function);
}

std::ostream& operator<<(std::ostream& stream, const OperatorType opt) {
  return stream << operator_type_to_string.left.at(opt);
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

std::ostream& operator<<(std::ostream& stream, const VectorCompressionType vector_compression_type) {
  return stream << vector_compression_type_to_string.left.at(vector_compression_type);
}

std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type) {
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedSize4ByteAligned: {
      stream << "FixedSize4ByteAligned";
      break;
    }
    case CompressedVectorType::FixedSize2ByteAligned: {
      stream << "FixedSize2ByteAligned";
      break;
    }
    case CompressedVectorType::FixedSize1ByteAligned: {
      stream << "FixedSize1ByteAligned";
      break;
    }
    case CompressedVectorType::SimdBp128: {
      stream << "SimdBp128";
      break;
    }
    default:
      break;
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const ColumnDataDistribution column_data_distribution) {
  stream << data_distribution_type_to_string.left.at(column_data_distribution.distribution_type) << "_";
  switch (column_data_distribution.distribution_type) {
    case DataDistributionType::Uniform:
      stream << column_data_distribution.min_value << "_" << column_data_distribution.max_value;
      break;
    case DataDistributionType::NormalSkewed:
      stream << column_data_distribution.skew_location << "_" << column_data_distribution.skew_scale << "_"
             << column_data_distribution.skew_shape;
      break;
    case DataDistributionType::Pareto:
      stream << column_data_distribution.pareto_scale << "_" << column_data_distribution.pareto_shape;
      break;
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const SegmentEncodingSpec spec) {
  stream << spec.encoding_type;
  if (spec.vector_compression_type) {
    stream << "-" << *spec.vector_compression_type;
  }

  return stream;
}

}  // namespace opossum
