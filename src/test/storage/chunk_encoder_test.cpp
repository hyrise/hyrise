#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
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
    static const auto row_count = 15u;
    static const auto max_chunk_size = 5u;
    static const auto column_count = 3u;

    _table = create_test_table(row_count, max_chunk_size, column_count);
  }

  static std::shared_ptr<Table> create_test_table(const size_t row_count, const size_t max_chunk_size,
                                                  const size_t column_count) {
    TableColumnDefinitions column_definitions;

    for (auto column_id = 0u; column_id < column_count; ++column_id) {
      const auto column_name = std::to_string(column_id);
      column_definitions.emplace_back(column_name, DataType::Int, false);
    }
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, max_chunk_size);

    for (auto row_id = 0u; row_id < row_count; ++row_id) {
      const auto row = std::vector<AllTypeVariant>(column_count, AllTypeVariant{static_cast<int32_t>(row_id)});
      table->append(row);
    }

    return table;
  }

 protected:
  std::shared_ptr<Table> _table;
};

TEST_F(ChunkEncoderTest, EncodeSingleChunk) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Dictionary}, {EncodingType::RunLength}, {EncodingType::Dictionary}};

  const auto types = _table->column_data_types();
  const auto column_count = _table->column_count();
  const auto row_count = _table->row_count();
  const auto chunk = _table->get_chunk(ChunkID{0u});

  ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec);

  EXPECT_EQ(types, _table->column_data_types());
  EXPECT_EQ(column_count, _table->column_count());
  EXPECT_EQ(row_count, _table->row_count());
  assert_chunk_encoding(chunk, chunk_encoding_spec);

  // Re-encoding with the same configuration
  ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec);
  assert_chunk_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, LeaveOneSegmentUnencoded) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Unencoded}, {EncodingType::RunLength}, {EncodingType::Dictionary}};

  const auto types = _table->column_data_types();
  const auto chunk = _table->get_chunk(ChunkID{0u});

  ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec);

  assert_chunk_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, UnencodeEncodedSegments) {
  const auto types = _table->column_data_types();
  const auto chunk = _table->get_chunk(ChunkID{0u});

  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Dictionary}, {EncodingType::RunLength}, {EncodingType::LZ4}};
  ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec);
  assert_chunk_encoding(chunk, chunk_encoding_spec);

  const auto chunk_unencoding_spec =
      ChunkEncodingSpec{{EncodingType::Unencoded}, {EncodingType::Unencoded}, {EncodingType::Unencoded}};
  ChunkEncoder::encode_chunk(chunk, types, chunk_unencoding_spec);
  assert_chunk_encoding(chunk, chunk_unencoding_spec);
}

TEST_F(ChunkEncoderTest, ThrowOnEncodingReferenceSegments) {
  auto table_wrapper = std::make_shared<TableWrapper>(_table);
  table_wrapper->execute();

  auto a = PQPColumnExpression::from_table(*_table, "0");
  auto table_scan = std::make_shared<TableScan>(table_wrapper, greater_than_equals_(a, 0));
  table_scan->execute();

  EXPECT_EQ(_table->row_count(), table_scan->get_output()->row_count());

  const auto chunk_encoding_spec =
      ChunkEncodingSpec{{EncodingType::Dictionary}, {EncodingType::Dictionary}, {EncodingType::Dictionary}};
  auto chunk = std::const_pointer_cast<Chunk>(table_scan->get_output()->get_chunk(ChunkID{0u}));
  const auto types = _table->column_data_types();
  EXPECT_THROW(ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec), std::logic_error);
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
    assert_chunk_encoding(chunk, spec);
  }
}

TEST_F(ChunkEncoderTest, EncodeWholeTableUsingSameEncoding) {
  const auto segment_encoding_spec = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3u, segment_encoding_spec};

  ChunkEncoder::encode_all_chunks(_table, segment_encoding_spec);

  for (auto chunk_id = ChunkID{0u}; chunk_id < _table->chunk_count(); ++chunk_id) {
    const auto chunk = _table->get_chunk(chunk_id);
    assert_chunk_encoding(chunk, chunk_encoding_spec);
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
    assert_chunk_encoding(chunk, spec);
  }

  const auto unencoded_chunk_spec =
      ChunkEncodingSpec{{EncodingType::Unencoded}, {EncodingType::Unencoded}, {EncodingType::Unencoded}};

  assert_chunk_encoding(_table->get_chunk(ChunkID{1u}), unencoded_chunk_spec);
}

TEST_F(ChunkEncoderTest, EncodeMultipleChunksUsingSameEncoding) {
  const auto chunk_ids = std::vector<ChunkID>{ChunkID{0u}, ChunkID{2u}};

  const auto segment_encoding_spec = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3u, segment_encoding_spec};

  ChunkEncoder::encode_chunks(_table, chunk_ids, segment_encoding_spec);

  for (auto chunk_id : chunk_ids) {
    const auto chunk = _table->get_chunk(chunk_id);
    assert_chunk_encoding(chunk, chunk_encoding_spec);
  }

  const auto unencoded_chunk_spec = ChunkEncodingSpec{3u, SegmentEncodingSpec{EncodingType::Unencoded}};

  assert_chunk_encoding(_table->get_chunk(ChunkID{1u}), unencoded_chunk_spec);
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
      assert_chunk_encoding(_table->get_chunk(chunk_id), chunk_encoding_spec);
    }
  }
}

}  // namespace opossum
