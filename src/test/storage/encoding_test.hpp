#pragma once

#include <memory>

#include <boost/algorithm/string/classification.hpp>
#include <boost/range/algorithm_ext/erase.hpp>

#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

// Base Class for tests that should be run with various encodings
class EncodingTest : public ::testing::TestWithParam<SegmentEncodingSpec> {
 public:
  std::shared_ptr<Table> load_table_with_encoding(const std::string& path,
                                                  ChunkOffset max_chunk_size = Chunk::DEFAULT_SIZE) {
    const auto table = load_table(path, max_chunk_size);

    auto chunk_encoding_spec = ChunkEncodingSpec{table->column_count(), EncodingType::Unencoded};

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      if (encoding_supports_data_type(GetParam().encoding_type, table->column_data_type(column_id))) {
        chunk_encoding_spec[column_id] = GetParam();
      }
    }

    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
    return table;
  }
};

inline std::string all_segment_encoding_specs_formatter(
    const testing::TestParamInfo<EncodingTest::ParamType>& param_info) {
  std::stringstream stringstream;
  stringstream << param_info.param;
  auto string = stringstream.str();
  boost::remove_erase_if(string, boost::is_any_of("() -"));
  return string;
}

const SegmentEncodingSpec all_segment_encoding_specs[]{
    {EncodingType::Unencoded},
    {EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
    {EncodingType::Dictionary, VectorCompressionType::SimdBp128},
    {EncodingType::FrameOfReference},
    {EncodingType::LZ4, VectorCompressionType::FixedSizeByteAligned},
    {EncodingType::LZ4, VectorCompressionType::SimdBp128},
    {EncodingType::RunLength}};

inline std::vector<SegmentEncodingSpec> get_supporting_segment_encodings_specs(const DataType data_type,
                                                                               const bool include_unencoded = true) {
  std::vector<SegmentEncodingSpec> segment_encodings;
  for (const auto& spec : all_segment_encoding_specs) {
    // Add all encoding types to the returned vector if they support the given data type. As some test cases work on
    // encoded segments only, it is further tested if the segment is not encoded and if segments of type Unencoded
    // should be included or not (flag `include_unencoded`).
    if (encoding_supports_data_type(spec.encoding_type, data_type) &&
        (spec.encoding_type != EncodingType::Unencoded || include_unencoded)) {
      segment_encodings.emplace_back(spec);
    }
  }
  return segment_encodings;
}

}  // namespace opossum
