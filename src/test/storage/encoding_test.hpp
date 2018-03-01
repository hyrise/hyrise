#pragma once

#include <memory>

#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class EncodingTest : public ::testing::TestWithParam<ColumnEncodingSpec> {
 public:
  std::shared_ptr<Table> load_table_with_encoding(const std::string& path, ChunkOffset max_chunk_size) {
    const auto table = load_table(path, max_chunk_size);
    if (GetParam().encoding_type != EncodingType::Unencoded) {
      ChunkEncoder::encode_all_chunks(table, GetParam());
    }
    return table;
  }
};

// clang-format off
#define INSTANCIATE_ENCODING_TYPE_TESTS(Test) \
  INSTANTIATE_TEST_CASE_P(Instances##Test, Test, ::testing::Values( \
    ColumnEncodingSpec(EncodingType::Unencoded), \
    ColumnEncodingSpec(EncodingType::DeprecatedDictionary), \
    ColumnEncodingSpec(EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned), \
    ColumnEncodingSpec(EncodingType::Dictionary, VectorCompressionType::SimdBp128), \
    ColumnEncodingSpec(EncodingType::Dictionary, VectorCompressionType::SimdBp128), \
    ColumnEncodingSpec(EncodingType::RunLength) \
  ),)
// clang-format on

}  // namespace opossum
