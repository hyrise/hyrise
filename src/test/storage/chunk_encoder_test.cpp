#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"

namespace opossum {

class ChunkEncoderTest : public BaseTest {
 public:
  void SetUp() override {
    static const auto max_chunk_size = 5u;

    static const auto column_count = 3u;
    TableColumnDefinitions column_definitions;
    for (auto column_id = 0u; column_id < column_count; ++column_id) {
      const auto column_name = std::to_string(column_id);
      column_definitions.emplace_back(column_name, DataType::Int);
    }
    _table = std::make_shared<Table>(column_definitions, TableType::Data, max_chunk_size);

    static const auto row_count = max_chunk_size * 3u;
    for (auto row_id = 0u; row_id < row_count; ++row_id) {
      const auto row = std::vector<AllTypeVariant>(column_count, AllTypeVariant{static_cast<int32_t>(row_id)});
      _table->append(row);
    }
  }

 protected:
  void verify_encoding(const std::shared_ptr<Chunk>& chunk, const ChunkEncodingSpec& spec) {
    for (auto column_id = ColumnID{0u}; column_id < chunk->column_count(); ++column_id) {
      const auto segment = chunk->get_segment(column_id);
      const auto segment_spec = spec.at(column_id);

      if (segment_spec.encoding_type == EncodingType::Unencoded) {
        const auto value_segment = std::dynamic_pointer_cast<const BaseValueSegment>(segment);
        EXPECT_NE(value_segment, nullptr);
      } else {
        const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
        EXPECT_NE(encoded_segment, nullptr);
        EXPECT_EQ(encoded_segment->encoding_type(), segment_spec.encoding_type);
        if (segment_spec.vector_compression_type) {
          EXPECT_EQ(*segment_spec.vector_compression_type,
                    parent_vector_compression_type(*encoded_segment->compressed_vector_type()));
        }
      }
    }
  }

 protected:
  std::shared_ptr<Table> _table;
};

TEST_F(ChunkEncoderTest, EncodeSingleChunk) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Dictionary}, {EncodingType::RunLength}, {EncodingType::Dictionary}};

  auto types = _table->column_data_types();
  const auto chunk = _table->get_chunk(ChunkID{0u});

  ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec);

  verify_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, LeaveOneSegmentUnencoded) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Unencoded}, {EncodingType::RunLength}, {EncodingType::Dictionary}};

  auto types = _table->column_data_types();
  const auto chunk = _table->get_chunk(ChunkID{0u});

  ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec);

  verify_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, EncodeWholeTable) {
  const auto chunk_encoding_specs = std::vector<ChunkEncodingSpec>{
      {{EncodingType::Unencoded}, {EncodingType::RunLength}, {EncodingType::Dictionary}},
      {{EncodingType::RunLength}, {EncodingType::RunLength}, {EncodingType::Dictionary}},
      {{EncodingType::Dictionary}, {EncodingType::RunLength}, {EncodingType::Dictionary}}};

  ChunkEncoder::encode_all_chunks(_table, chunk_encoding_specs);

  for (auto chunk_id = ChunkID{0u}; chunk_id < _table->chunk_count(); ++chunk_id) {
    const auto chunk = _table->get_chunk(chunk_id);
    const auto& spec = chunk_encoding_specs.at(chunk_id);
    verify_encoding(chunk, spec);
  }
}

TEST_F(ChunkEncoderTest, EncodeWholeTableUsingSameEncoding) {
  const auto segment_encoding_spec = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3u, segment_encoding_spec};

  ChunkEncoder::encode_all_chunks(_table, segment_encoding_spec);

  for (auto chunk_id = ChunkID{0u}; chunk_id < _table->chunk_count(); ++chunk_id) {
    const auto chunk = _table->get_chunk(chunk_id);
    verify_encoding(chunk, chunk_encoding_spec);
  }
}

TEST_F(ChunkEncoderTest, EncodeMultipleChunks) {
  const auto chunk_ids = std::vector<ChunkID>{ChunkID{0u}, ChunkID{2u}};

  const auto chunk_encoding_specs = std::map<ChunkID, ChunkEncodingSpec>{
      {ChunkID{0u}, {{EncodingType::Unencoded}, {EncodingType::RunLength}, {EncodingType::Unencoded}}},
      {ChunkID{2u}, {{EncodingType::Dictionary}, {EncodingType::Dictionary}, {EncodingType::RunLength}}}};

  ChunkEncoder::encode_chunks(_table, chunk_ids, chunk_encoding_specs);

  for (auto chunk_id : chunk_ids) {
    const auto chunk = _table->get_chunk(chunk_id);
    const auto& spec = chunk_encoding_specs.at(chunk_id);
    verify_encoding(chunk, spec);
  }

  const auto unencoded_chunk_spec =
      ChunkEncodingSpec{{EncodingType::Unencoded}, {EncodingType::Unencoded}, {EncodingType::Unencoded}};

  verify_encoding(_table->get_chunk(ChunkID{1u}), unencoded_chunk_spec);
}

TEST_F(ChunkEncoderTest, EncodeMultipleChunksUsingSameEncoding) {
  const auto chunk_ids = std::vector<ChunkID>{ChunkID{0u}, ChunkID{2u}};

  const auto segment_encoding_spec = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3u, segment_encoding_spec};

  ChunkEncoder::encode_chunks(_table, chunk_ids, segment_encoding_spec);

  for (auto chunk_id : chunk_ids) {
    const auto chunk = _table->get_chunk(chunk_id);
    verify_encoding(chunk, chunk_encoding_spec);
  }

  const auto unencoded_chunk_spec = ChunkEncodingSpec{3u, SegmentEncodingSpec{EncodingType::Unencoded}};

  verify_encoding(_table->get_chunk(ChunkID{1u}), unencoded_chunk_spec);
}

TEST_F(ChunkEncoderTest, ReencodingTable) {
  // Encoding specifications which will be applied one after another to the chunk.
  const auto chunk_encoding_specs =
      std::vector<ChunkEncodingSpec>{{{EncodingType::Unencoded},
                                      {EncodingType::RunLength},
                                      {EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned}},
                                     {{EncodingType::Unencoded},
                                      {EncodingType::RunLength},
                                      {EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned}},
                                     {{EncodingType::Dictionary},
                                      {EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
                                      {EncodingType::Dictionary, VectorCompressionType::SimdBp128}},
                                     {{EncodingType::Unencoded}, {EncodingType::Unencoded}, {EncodingType::Unencoded}}};
  const auto types = _table->column_data_types();

  for (auto const& chunk_encoding_spec : chunk_encoding_specs) {
    ChunkEncoder::encode_all_chunks(_table, chunk_encoding_spec);
    const auto chunk_count = _table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      verify_encoding(_table->get_chunk(chunk_id), chunk_encoding_spec);
    }
  }
}

}  // namespace opossum
