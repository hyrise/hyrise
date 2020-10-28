#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "import_export/binary/binary_writer.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BinaryWriterTest : public BaseTest {
 protected:
  void SetUp() override { std::remove(filename.c_str()); }

  void TearDown() override { std::remove(filename.c_str()); }

  std::shared_ptr<Table> table;
  const std::string filename = test_data_path + "export_test.bin";
  const std::string reference_filepath = "resources/test_data/bin/";
};

class BinaryWriterMultiEncodingTest : public BinaryWriterTest, public ::testing::WithParamInterface<EncodingType> {};

auto export_binary_formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  auto stream = std::stringstream{};
  stream << info.param;

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(BinaryEncodingTypes, BinaryWriterMultiEncodingTest,
                         ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength,
                                           EncodingType::LZ4),
                         export_binary_formatter);

TEST_F(BinaryWriterTest, TwoColumnsNoValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("FirstColumn", DataType::Int, false);
  column_definitions.emplace_back("SecondColumn", DataType::String, false);

  table = std::make_shared<Table>(column_definitions, TableType::Data, 30000);
  BinaryWriter::write(*table, filename);

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

TEST_F(BinaryWriterTest, FixedStringDictionarySingleChunk) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
  BinaryWriter::write(*table, filename);

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

TEST_F(BinaryWriterTest, FixedStringDictionaryNullValue) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({opossum::NULL_VALUE});
  table->append({"test"});
  table->append({opossum::NULL_VALUE});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
  BinaryWriter::write(*table, filename);

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

TEST_F(BinaryWriterTest, FixedStringDictionaryMultipleChunks) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
  BinaryWriter::write(*table, filename);

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

TEST_F(BinaryWriterTest, NullValuesFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({1});
  table->append({opossum::NULL_VALUE});
  table->append({2});
  table->append({opossum::NULL_VALUE});
  table->append({5});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FrameOfReference});
  BinaryWriter::write(*table, filename);

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

// A table with reference segments is materialized while exporting. The content of the export file should not be
// different from a exported table with ValueSegments and the same content.
// It assumes that the TableScan produces one output segment per input segment.
TEST_F(BinaryWriterTest, AllTypesReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = create_table_scan(table_wrapper, ColumnID{1}, PredicateCondition::NotEquals, 5);
  scan->execute();

  BinaryWriter::write(*(scan->get_output()), filename);

  EXPECT_TRUE(file_exists(filename));

  // Because reference tables are materialized and stored as if they were value segments, the binary dump will
  // contain the default chunk size.
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

TEST_F(BinaryWriterTest, SingleChunkFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({1});
  table->append({2});
  table->append({3});
  table->append({4});
  table->append({5});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FrameOfReference});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_F(BinaryWriterTest, MultipleChunksFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({1});
  table->append({1});
  table->append({2});
  table->append({4});
  table->append({5});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FrameOfReference});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_F(BinaryWriterTest, AllNullFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({opossum::NULL_VALUE});
  table->append({opossum::NULL_VALUE});
  table->append({opossum::NULL_VALUE});
  table->append({opossum::NULL_VALUE});
  table->append({opossum::NULL_VALUE});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::FrameOfReference});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_F(BinaryWriterTest, LZ4MultipleBlocks) {
  // Export more rows than minimum block size of 16384
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 20000);

  for (int index = 0; index < 5000; ++index) {
    table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
    table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
    table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
    table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});
  }

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::LZ4});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_F(BinaryWriterTest, SortColumnDefinitions) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  // Chunk 0: a sorted ascending, b sorted descending
  table->append({1, 3});
  table->append({2, 2});
  table->append({3, 1});
  // Chunk 1: a not sorted, b sorted descending
  table->append({1, 3});
  table->append({2, 2});
  table->append({1, 1});
  // Chunk 2: a and b not sorted
  table->append({1, 1});
  table->append({2, 2});
  table->append({1, 1});

  table->last_chunk()->finalize();

  // Set sorted by information
  const auto chunk_0_sorted_columns = std::vector<SortColumnDefinition>{
      SortColumnDefinition{ColumnID{0}}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}};
  const auto chunk_1_sorted_columns =
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{1}, SortMode::Descending}};
  table->get_chunk(ChunkID{0})->set_individually_sorted_by(chunk_0_sorted_columns);
  table->get_chunk(ChunkID{1})->set_individually_sorted_by(chunk_1_sorted_columns);

  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

// TEST_P for all supported encoding types

TEST_P(BinaryWriterMultiEncodingTest, RepeatedInt) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({1});
  table->append({2});
  table->append({2});
  table->append({2});
  table->append({2});
  table->append({1});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin", filename));
}

TEST_P(BinaryWriterMultiEncodingTest, SingleChunkSingleFloatColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Float, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  table->append({5.5f});
  table->append({13.0f});
  table->append({16.2f});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, MultipleChunkSingleFloatColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Float, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
  table->append({5.5f});
  table->append({13.0f});
  table->append({16.2f});

  BinaryWriter::write(*table, filename);

  std::string reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, StringSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, AllTypesSegmentSorted) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, AllTypesSegmentUnsorted) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, AllTypesMixColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  ChunkEncoder::encode_chunks(table, {ChunkID{0}}, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, EmptyStringsSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_chunks(table, {ChunkID{0}}, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, AllTypesNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  table->append({opossum::NULL_VALUE, 1.1f, int64_t{100}, "one", 1.11});
  table->append({2, opossum::NULL_VALUE, int64_t{200}, "two", 2.22});
  table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  table->append({4, 4.4f, int64_t{400}, opossum::NULL_VALUE, 4.44});
  table->append({5, 5.5f, int64_t{500}, "five", opossum::NULL_VALUE});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, AllTypesAllNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 100'000);
  auto null_values = {opossum::NULL_VALUE, opossum::NULL_VALUE, opossum::NULL_VALUE, opossum::NULL_VALUE,
                      opossum::NULL_VALUE};

  table->append(null_values);
  table->append(null_values);
  table->append(null_values);
  table->append(null_values);
  table->append(null_values);

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_P(BinaryWriterMultiEncodingTest, RunNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);

  auto values = pmr_vector<int32_t>{1, 1, 1, 1, 2, 2, 2, 3};
  auto null_values = pmr_vector<bool>{true, false, true, true, true, false, false, true};
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  table->append_chunk(Segments{value_segment});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{GetParam()});
  BinaryWriter::write(*table, filename);

  const auto reference_filename =
      reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

}  // namespace opossum
