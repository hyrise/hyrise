#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "operators/import_binary.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

class OperatorsImportBinaryTest : public BaseTest {};

class OperatorsImportBinaryMultiEncodingTest : public OperatorsImportBinaryTest,
                                               public ::testing::WithParamInterface<EncodingType> {};

auto formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  auto stream = std::stringstream{};
  stream << info.param;

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(BinaryEncodingTypes, OperatorsImportBinaryMultiEncodingTest,
                         ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength),
                         formatter);

TEST_P(OperatorsImportBinaryMultiEncodingTest, SingleChunkSingleFloatColumn) {
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, 5);
  expected_table->append({5.5f});
  expected_table->append({13.0f});
  expected_table->append({16.2f});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, GetParam());

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/SingleChunkSingleFloatColumnValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/SingleChunkSingleFloatColumnDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/SingleChunkSingleFloatColumnRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_P(OperatorsImportBinaryMultiEncodingTest, MultipleChunkSingleFloatColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Float, false);
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
  expected_table->append({5.5f});
  expected_table->append({13.0f});
  expected_table->append({16.2f});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, GetParam());

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/MultipleChunkSingleFloatColumnValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/MultipleChunkSingleFloatColumnDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/MultipleChunkSingleFloatColumnRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
  EXPECT_EQ(importer->get_output()->chunk_count(), 2u);
}

TEST_P(OperatorsImportBinaryMultiEncodingTest, StringSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 10, UseMvcc::Yes);
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, GetParam());

  Hyrise::get().storage_manager.add_table("table_a", expected_table);

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/StringValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/StringDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/StringRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_P(OperatorsImportBinaryMultiEncodingTest, AllTypesSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, GetParam());

  Hyrise::get().storage_manager.add_table("expected_table", expected_table);

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/AllTypesValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/AllTypesDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/AllTypesRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_P(OperatorsImportBinaryMultiEncodingTest, AllTypesMixColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_chunks(expected_table, {ChunkID{0}}, GetParam());

  Hyrise::get().storage_manager.add_table("expected_table", expected_table);

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/AllTypesMixColumnValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/AllTypesMixColumnDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/AllTypesMixColumnRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_P(OperatorsImportBinaryMultiEncodingTest, EmptyStringsSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 10);

  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, GetParam());

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/EmptyStringsValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/EmptyStringsDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/EmptyStringsRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_P(OperatorsImportBinaryMultiEncodingTest, AllTypesNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data);

  expected_table->append({opossum::NULL_VALUE, 1.1f, int64_t{100}, "one", 1.11});
  expected_table->append({2, opossum::NULL_VALUE, int64_t{200}, "two", 2.22});
  expected_table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  expected_table->append({4, 4.4f, int64_t{400}, opossum::NULL_VALUE, 4.44});
  expected_table->append({5, 5.5f, int64_t{500}, "five", opossum::NULL_VALUE});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, GetParam());

  std::map<EncodingType, std::string> reference_filenames{
      {EncodingType::Unencoded, "resources/test_data/bin/AllTypesNullValuesValueSegment.bin"},
      {EncodingType::Dictionary, "resources/test_data/bin/AllTypesNullValuesDictionarySegment.bin"},
      {EncodingType::RunLength, "resources/test_data/bin/AllTypesNullValuesRunLengthSegment.bin"}};
  auto importer = std::make_shared<opossum::ImportBinary>(reference_filenames.at(GetParam()));
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, FixedStringDictionarySegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, EncodingType::FixedStringDictionary);

  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/FixedStringDictionarySegment.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, FrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  expected_table->append({1});
  expected_table->append({2});
  expected_table->append({3});
  expected_table->append({4});
  expected_table->append({5});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, EncodingType::FrameOfReference);

  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/FrameOfReferenceSegment.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, MultipeChunksFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  expected_table->append({1});
  expected_table->append({1});
  expected_table->append({2});
  expected_table->append({4});
  expected_table->append({5});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, EncodingType::FrameOfReference);

  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/MultipleChunksFrameOfReferenceSegment.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, FileDoesNotExist) {
  auto importer = std::make_shared<opossum::ImportBinary>("not_existing_file");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportBinaryTest, TwoColumnsNoValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("FirstColumn", DataType::Int, false);
  column_definitions.emplace_back("SecondColumn", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 30000);

  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/TwoColumnsNoValues.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, RepeatedIntRunLengthSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 3);

  expected_table->append({1});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({1});

  expected_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(expected_table, EncodingType::RunLength);

  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/RepeatedIntRunLengthSegment.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, SaveToStorageManager) {
  auto importer =
      std::make_shared<opossum::ImportBinary>("resources/test_data/bin/float.bin", std::string("float_table"));
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("float_table"), expected_table);
}

TEST_F(OperatorsImportBinaryTest, FallbackToRetrieveFromStorageManager) {
  auto importer =
      std::make_shared<opossum::ImportBinary>("resources/test_data/bin/float.bin", std::string("float_table"));
  importer->execute();
  auto retriever = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/AllTypesMixColumn.bin",
                                                           std::string("float_table"));
  retriever->execute();
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), retriever->get_output());
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("float_table"), retriever->get_output());
}

TEST_F(OperatorsImportBinaryTest, InvalidColumnType) {
  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/InvalidColumnType.bin",
                                                          std::string("float_table"));
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportBinaryTest, InvalidAttributeVectorWidth) {
  auto importer = std::make_shared<opossum::ImportBinary>("resources/test_data/bin/InvalidAttributeVectorWidth.bin",
                                                          std::string("float_table"));
  EXPECT_THROW(importer->execute(), std::exception);
}

}  // namespace opossum
