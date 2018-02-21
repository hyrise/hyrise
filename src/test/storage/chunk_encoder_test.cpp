#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "storage/base_encoded_column.hpp"
#include "storage/base_value_column.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

namespace opossum {

class ChunkEncoderTest : public BaseTest {
 public:
  void SetUp() override {
    static const auto max_chunk_size = 5u;
    _table = std::make_shared<Table>(max_chunk_size);

    static const auto column_count = 3u;
    for (auto column_id = 0u; column_id < column_count; ++column_id) {
      const auto column_name = std::to_string(column_id);
      _table->add_column(column_name, DataType::Int);
    }

    static const auto row_count = max_chunk_size * 3u;
    for (auto row_id = 0u; row_id < row_count; ++row_id) {
      const auto row = std::vector<AllTypeVariant>(column_count, AllTypeVariant{static_cast<int32_t>(row_id)});
      _table->append(row);
    }
  }

 protected:
  void verify_encoding(const std::shared_ptr<Chunk>& chunk, const ChunkEncodingSpec& spec) {
    for (auto column_id = ColumnID{0u}; column_id < chunk->column_count(); ++column_id) {
      const auto column = chunk->get_column(column_id);
      const auto column_spec = spec.at(column_id);

      if (column_spec.encoding_type == EncodingType::Unencoded) {
        const auto value_column = std::dynamic_pointer_cast<const BaseValueColumn>(column);
        EXPECT_NE(value_column, nullptr);
      } else {
        const auto encoded_column = std::dynamic_pointer_cast<const BaseEncodedColumn>(column);
        EXPECT_NE(encoded_column, nullptr);
        EXPECT_EQ(encoded_column->encoding_type(), column_spec.encoding_type);
      }
    }
  }

 protected:
  std::shared_ptr<Table> _table;
};

TEST_F(ChunkEncoderTest, EncodeSingleChunk) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Dictionary}, {EncodingType::RunLength}, {EncodingType::Dictionary}};

  auto data_types = _table->column_types();
  auto chunk = _table->get_chunk(ChunkID{0u});

  ChunkEncoder::encode_chunk(chunk, data_types, chunk_encoding_spec);

  verify_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, LeaveOneColumnUnencoded) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Unencoded}, {EncodingType::RunLength}, {EncodingType::Dictionary}};

  auto data_types = _table->column_types();
  auto chunk = _table->get_chunk(ChunkID{0u});

  ChunkEncoder::encode_chunk(chunk, data_types, chunk_encoding_spec);

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
  const auto column_encoding_spec = ColumnEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3u, column_encoding_spec};

  ChunkEncoder::encode_all_chunks(_table, column_encoding_spec);

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

  const auto column_encoding_spec = ColumnEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3u, column_encoding_spec};

  ChunkEncoder::encode_chunks(_table, chunk_ids, column_encoding_spec);

  for (auto chunk_id : chunk_ids) {
    const auto chunk = _table->get_chunk(chunk_id);
    verify_encoding(chunk, chunk_encoding_spec);
  }

  const auto unencoded_chunk_spec = ChunkEncodingSpec{3u, ColumnEncodingSpec{EncodingType::Unencoded}};

  verify_encoding(_table->get_chunk(ChunkID{1u}), unencoded_chunk_spec);
}

}  // namespace opossum
