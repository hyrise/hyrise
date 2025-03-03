#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"

namespace hyrise {

class ChunkEncoderTest : public BaseTest {
 public:
  void SetUp() override {
    static const auto row_count = size_t{15};
    static const auto target_chunk_size = ChunkOffset{5};
    static const auto column_count = ColumnCount{3};

    _table = create_test_table(row_count, target_chunk_size, column_count);
  }

  static std::shared_ptr<Table> create_test_table(const size_t row_count, const ChunkOffset target_chunk_size,
                                                  const ColumnCount column_count) {
    TableColumnDefinitions column_definitions;

    for (auto column_id = ColumnCount{0}; column_id < column_count; ++column_id) {
      const auto column_name = std::to_string(column_id);
      column_definitions.emplace_back(column_name, DataType::Int, false);
    }
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, target_chunk_size);

    for (auto row_id = size_t{0}; row_id < row_count; ++row_id) {
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
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::RunLength},
                        SegmentEncodingSpec{EncodingType::Dictionary}};

  const auto column_data_types = _table->column_data_types();
  const auto column_count = _table->column_count();
  const auto row_count = _table->row_count();
  const auto chunk = _table->get_chunk(ChunkID{0});

  ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec);

  EXPECT_EQ(column_data_types, _table->column_data_types());
  EXPECT_EQ(column_count, _table->column_count());
  EXPECT_EQ(row_count, _table->row_count());
  assert_chunk_encoding(chunk, chunk_encoding_spec);

  // Re-encoding with the same configuration
  ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec);
  assert_chunk_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, LeaveOneSegmentUnencoded) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::RunLength},
                        SegmentEncodingSpec{EncodingType::Dictionary}};

  const auto column_data_types = _table->column_data_types();
  const auto chunk = _table->get_chunk(ChunkID{0});

  ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec);

  assert_chunk_encoding(chunk, chunk_encoding_spec);
}

TEST_F(ChunkEncoderTest, UnencodeEncodedSegments) {
  const auto column_data_types = _table->column_data_types();
  const auto chunk = _table->get_chunk(ChunkID{0});

  const auto chunk_encoding_spec =
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::RunLength},
                        SegmentEncodingSpec{EncodingType::LZ4}};
  ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec);
  assert_chunk_encoding(chunk, chunk_encoding_spec);

  const auto chunk_unencoding_spec =
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::Unencoded},
                        SegmentEncodingSpec{EncodingType::Unencoded}};
  ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_unencoding_spec);
  assert_chunk_encoding(chunk, chunk_unencoding_spec);
}

TEST_F(ChunkEncoderTest, ThrowOnEncodingAMutableChunk) {
  const auto chunk_encoding_spec =
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::RunLength},
                        SegmentEncodingSpec{EncodingType::Dictionary}};
  const auto types = _table->column_data_types();

  // Appending a row should add a fourth, mutable chunk.
  _table->append({1, 2, 3});
  const auto& chunk = _table->get_chunk(ChunkID{3});

  EXPECT_THROW(ChunkEncoder::encode_chunk(chunk, types, chunk_encoding_spec), std::logic_error);
}

TEST_F(ChunkEncoderTest, ThrowOnEncodingReferenceSegments) {
  auto table_wrapper = std::make_shared<TableWrapper>(_table);
  table_wrapper->execute();

  auto a = PQPColumnExpression::from_table(*_table, "0");
  auto table_scan = std::make_shared<TableScan>(table_wrapper, greater_than_equals_(a, 0));
  table_scan->execute();

  EXPECT_EQ(_table->row_count(), table_scan->get_output()->row_count());

  const auto chunk_encoding_spec =
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Dictionary},
                        SegmentEncodingSpec{EncodingType::Dictionary}};
  auto chunk = std::const_pointer_cast<Chunk>(table_scan->get_output()->get_chunk(ChunkID{0}));
  const auto column_data_types = _table->column_data_types();
  EXPECT_THROW(ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec), std::logic_error);
}

TEST_F(ChunkEncoderTest, EncodeWholeTable) {
  const auto chunk_encoding_specs = std::vector<ChunkEncodingSpec>{
      {SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::RunLength},
       SegmentEncodingSpec{EncodingType::Dictionary}},
      {SegmentEncodingSpec{EncodingType::RunLength}, SegmentEncodingSpec{EncodingType::RunLength},
       SegmentEncodingSpec{EncodingType::Dictionary}},
      {SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::RunLength},
       SegmentEncodingSpec{EncodingType::Dictionary}}};

  _table->last_chunk()->set_immutable();
  ChunkEncoder::encode_all_chunks(_table, chunk_encoding_specs);

  for (auto chunk_id = ChunkID{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
    const auto chunk = _table->get_chunk(chunk_id);
    const auto& spec = chunk_encoding_specs.at(chunk_id);
    assert_chunk_encoding(chunk, spec);
  }
}

TEST_F(ChunkEncoderTest, EncodeWholeTableUsingSameEncoding) {
  const auto segment_encoding_spec = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3, segment_encoding_spec};

  _table->last_chunk()->set_immutable();
  ChunkEncoder::encode_all_chunks(_table, segment_encoding_spec);

  for (auto chunk_id = ChunkID{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
    const auto chunk = _table->get_chunk(chunk_id);
    assert_chunk_encoding(chunk, chunk_encoding_spec);
  }
}

TEST_F(ChunkEncoderTest, EncodeMultipleChunks) {
  const auto chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{2}};

  const auto chunk_encoding_specs = std::map<ChunkID, ChunkEncodingSpec>{
      {ChunkID{0},
       {SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::RunLength},
        SegmentEncodingSpec{EncodingType::Unencoded}}},
      {ChunkID{2},
       {SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Dictionary},
        SegmentEncodingSpec{EncodingType::RunLength}}}};

  _table->get_chunk(static_cast<ChunkID>(ChunkID{2}))->set_immutable();
  ChunkEncoder::encode_chunks(_table, chunk_ids, chunk_encoding_specs);

  for (auto chunk_id : chunk_ids) {
    const auto chunk = _table->get_chunk(chunk_id);
    const auto& spec = chunk_encoding_specs.at(chunk_id);
    assert_chunk_encoding(chunk, spec);
  }

  const auto unencoded_chunk_spec =
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::Unencoded},
                        SegmentEncodingSpec{EncodingType::Unencoded}};

  assert_chunk_encoding(_table->get_chunk(ChunkID{1}), unencoded_chunk_spec);
}

TEST_F(ChunkEncoderTest, EncodeMultipleChunksUsingSameEncoding) {
  const auto chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{2}};

  const auto segment_encoding_spec = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto chunk_encoding_spec = ChunkEncodingSpec{3, segment_encoding_spec};

  _table->get_chunk(static_cast<ChunkID>(ChunkID{2}))->set_immutable();
  ChunkEncoder::encode_chunks(_table, chunk_ids, segment_encoding_spec);

  for (auto chunk_id : chunk_ids) {
    const auto chunk = _table->get_chunk(chunk_id);
    assert_chunk_encoding(chunk, chunk_encoding_spec);
  }

  const auto unencoded_chunk_spec = ChunkEncodingSpec{3, SegmentEncodingSpec{EncodingType::Unencoded}};

  assert_chunk_encoding(_table->get_chunk(ChunkID{1}), unencoded_chunk_spec);
}

TEST_F(ChunkEncoderTest, ReencodingTable) {
  // Encoding specifications which will be applied one after another to the chunk.
  const auto chunk_encoding_specs = std::vector<ChunkEncodingSpec>{
      {SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::RunLength},
       SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger}},
      {SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::RunLength},
       SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger}},
      {SegmentEncodingSpec{EncodingType::Dictionary},
       SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger},
       SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::BitPacking}},
      {SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::Unencoded},
       SegmentEncodingSpec{EncodingType::Unencoded}}};
  const auto column_data_types = _table->column_data_types();

  _table->last_chunk()->set_immutable();

  for (auto const& chunk_encoding_spec : chunk_encoding_specs) {
    ChunkEncoder::encode_all_chunks(_table, chunk_encoding_spec);
    const auto chunk_count = _table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      assert_chunk_encoding(_table->get_chunk(chunk_id), chunk_encoding_spec);
    }
  }
}

TEST_F(ChunkEncoderTest, ReencodeNotNullableSegment) {
  auto value_segment = std::make_shared<ValueSegment<int>>();
  value_segment->append(4);
  value_segment->append(6);
  value_segment->append(3);
  value_segment->append(4);

  auto dict_segment =
      ChunkEncoder::encode_segment(value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
  auto reencoded_segment =
      ChunkEncoder::encode_segment(dict_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Unencoded});
  const auto casted_reencoded_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(reencoded_segment);

  EXPECT_FALSE(casted_reencoded_segment->is_nullable());
}

}  // namespace hyrise
