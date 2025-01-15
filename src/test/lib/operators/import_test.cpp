#include <optional>
#include <tuple>

#include "magic_enum.hpp"

#include "base_test.hpp"
#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "operators/import.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"

namespace hyrise {

class OperatorsImportTest : public BaseTest {
 protected:
  const std::string reference_filepath = "resources/test_data/";
  const std::map<FileType, std::string> reference_filenames{
      {FileType::Binary, "bin/float"}, {FileType::Tbl, "tbl/float"}, {FileType::Csv, "csv/float"}};
  const std::map<FileType, std::string> file_extensions{
      {FileType::Binary, ".bin"}, {FileType::Tbl, ".tbl"}, {FileType::Csv, ".csv"}};
};

class OperatorsImportMultiFileTypeAndEncodingTest
    : public OperatorsImportTest,
      public ::testing::WithParamInterface<std::tuple<FileType, std::optional<EncodingType>>> {};

std::string file_types_and_encodings_name_generator(
    const ::testing::TestParamInfo<std::tuple<FileType, std::optional<EncodingType>>>& info) {
  auto [file_type, encoding_type] = info.param;

  // Convert FileType and EncodingType to strings for the name
  std::string file_type_str = std::string{magic_enum::enum_name(file_type)};

  std::string encoding_type_str = encoding_type ? std::string{magic_enum::enum_name(*encoding_type)} : "None";

  return file_type_str + "_" + encoding_type_str;
}

INSTANTIATE_TEST_SUITE_P(
    FileTypesAndEncodings, OperatorsImportMultiFileTypeAndEncodingTest,
    ::testing::Combine(
        ::testing::Values(FileType::Csv, FileType::Tbl, FileType::Binary),
        ::testing::Values(
            std::nullopt, EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength,
            // EncodingType::FixedStringDictionary, // Both FixedStringDictionary and FrameOfReference do not work with float.
            // EncodingType::FrameOfReference,
            EncodingType::LZ4)),
    file_types_and_encodings_name_generator);  // add function to format tuples into strings

TEST_P(OperatorsImportMultiFileTypeAndEncodingTest, ImportWithFileType) {
  const auto& [file_type, encoding] = GetParam();

  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, ChunkOffset{5});
  expected_table->append({1.1f});
  expected_table->append({2.2f});
  expected_table->append({3.3f});
  expected_table->append({4.4f});

  // Make all chunks in the expected table immutable to allow encoding.
  for (auto chunk_id = ChunkID{0}; chunk_id < expected_table->chunk_count(); ++chunk_id) {
    expected_table->get_chunk(chunk_id)->set_immutable();
  }

  if (encoding) {
    const auto encoding_spec = SegmentEncodingSpec{*encoding};
    ChunkEncoder::encode_all_chunks(expected_table, encoding_spec);
  }

  const auto reference_filename = reference_filepath + reference_filenames.at(file_type);
  auto importer = std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, file_type, encoding);
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

TEST_P(OperatorsImportMultiFileTypeAndEncodingTest, ImportWithoutFileType) {
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, ChunkOffset{5});
  expected_table->append({1.1f});
  expected_table->append({2.2f});
  expected_table->append({3.3f});
  expected_table->append({4.4f});

  // Make all chunks in the expected table immutable to allow encoding.
  for (auto chunk_id = ChunkID{0}; chunk_id < expected_table->chunk_count(); ++chunk_id) {
    expected_table->get_chunk(chunk_id)->set_immutable();
  }

  const auto& [file_type, encoding] = GetParam();

  if (encoding) {
    const auto encoding_spec = SegmentEncodingSpec{*encoding};
    ChunkEncoder::encode_all_chunks(expected_table, encoding_spec);
  }

  const auto reference_filename =
      reference_filepath + reference_filenames.at(file_type) + file_extensions.at(file_type);
  auto importer = std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, file_type, encoding);
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

TEST_P(OperatorsImportMultiFileTypeAndEncodingTest, ImportWithEncoding) {
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, ChunkOffset{5});
  expected_table->append({1.1f});
  expected_table->append({2.2f});
  expected_table->append({3.3f});
  expected_table->append({4.4f});

  // Make all chunks in the expected table immutable to allow encoding.
  for (auto chunk_id = ChunkID{0}; chunk_id < expected_table->chunk_count(); ++chunk_id) {
    expected_table->get_chunk(chunk_id)->set_immutable();
  }

  const auto& [file_type, encoding] = GetParam();

  if (encoding) {
    const auto encoding_spec = SegmentEncodingSpec{*encoding};
    ChunkEncoder::encode_all_chunks(expected_table, encoding_spec);
  }

  const auto reference_filename =
      reference_filepath + reference_filenames.at(file_type) + file_extensions.at(file_type);
  auto importer = std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, file_type, encoding);
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

TEST_P(OperatorsImportMultiFileTypeAndEncodingTest, HasCorrectMvccData) {
  const auto& [file_type, encoding] = GetParam();

  const auto reference_filename =
      reference_filepath + reference_filenames.at(file_type) + file_extensions.at(file_type);
  auto importer = std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, file_type, encoding);
  importer->execute();

  auto table = Hyrise::get().storage_manager.get_table("a");

  EXPECT_EQ(table->uses_mvcc(), UseMvcc::Yes);
  EXPECT_TRUE(table->get_chunk(ChunkID{0})->has_mvcc_data());
  EXPECT_EQ(table->get_chunk(ChunkID{0})->mvcc_data()->max_begin_cid.load(), CommitID{0});
}

TEST_F(OperatorsImportTest, EncodeImportWithFrameOfReference) {
  const auto& encoding_type = EncodingType::FrameOfReference;
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data, ChunkOffset{4});

  expected_table->append({123});
  expected_table->append({1234});
  expected_table->append({12345});

  // Make all chunks in the expected table immutable to allow encoding.
  for (auto chunk_id = ChunkID{0}; chunk_id < expected_table->chunk_count(); ++chunk_id) {
    expected_table->get_chunk(chunk_id)->set_immutable();
  }
  // Encode the expected table with FrameOfReference
  const auto encoding_spec = SegmentEncodingSpec{encoding_type};
  ChunkEncoder::encode_all_chunks(expected_table, encoding_spec);

  const auto filename = std::string{"resources/test_data/tbl/int.tbl"};
  auto importer = std::make_shared<Import>(filename, "a", Chunk::DEFAULT_SIZE, FileType::Tbl, encoding_type);
  importer->execute();

  auto table = Hyrise::get().storage_manager.get_table("a");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(OperatorsImportTest, EncodeImportWithFixedStringDictionary) {
  const auto encoding_type = EncodingType::FixedStringDictionary;
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::String, false}}, TableType::Data, ChunkOffset{6});

  expected_table->append({"xxx"});
  expected_table->append({"www"});
  expected_table->append({"yyy"});
  expected_table->append({"uuu"});
  expected_table->append({"ttt"});
  expected_table->append({"zzz"});

  // Make all chunks in the expected table immutable to allow encoding.
  for (auto chunk_id = ChunkID{0}; chunk_id < expected_table->chunk_count(); ++chunk_id) {
    expected_table->get_chunk(chunk_id)->set_immutable();
  }
  // Encode the expected table with FrameOfReference
  const auto encoding_spec = SegmentEncodingSpec{encoding_type};
  ChunkEncoder::encode_all_chunks(expected_table, encoding_spec);

  const auto filename = std::string{"resources/test_data/tbl/string.tbl"};
  auto importer = std::make_shared<Import>(filename, "a", Chunk::DEFAULT_SIZE, FileType::Tbl, encoding_type);
  importer->execute();

  auto table = Hyrise::get().storage_manager.get_table("a");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(OperatorsImportTest, FileDoesNotExist) {
  auto importer = std::make_shared<Import>("not_existing_file.tbl", "a");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportTest, UnknownFileExtension) {
  EXPECT_THROW(std::make_shared<Import>("not_existing_file.mp3", "a"), std::exception);
}

TEST_F(OperatorsImportTest, ReplaceExistingTable) {
  auto old_table = load_table("resources/test_data/tbl/float.tbl");
  Hyrise::get().storage_manager.add_table("a", old_table);

  auto expected_table = load_table("resources/test_data/tbl/int.tbl");
  auto importer = std::make_shared<Import>("resources/test_data/tbl/int.tbl", "a");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

TEST_F(OperatorsImportTest, ChunkSize) {
  auto importer = std::make_shared<Import>("resources/test_data/csv/float_int_large.csv", "a", ChunkOffset{20});
  importer->execute();

  // check if chunk_size property is correct
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->target_chunk_size(), 20U);

  // check if actual chunk_size is correct
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->get_chunk(ChunkID{0})->size(), 20U);
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->get_chunk(ChunkID{1})->size(), 20U);
}

TEST_F(OperatorsImportTest, TargetChunkSize) {
  auto importer =
      std::make_shared<Import>("resources/test_data/csv/float_int_large_chunksize_max.csv", "a", Chunk::DEFAULT_SIZE);
  importer->execute();

  // check if chunk_size property is correct (target chunk size)
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->target_chunk_size(), Chunk::DEFAULT_SIZE);

  // check if actual chunk_size and chunk_count is correct
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->get_chunk(ChunkID{0})->size(), 100U);
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->chunk_count(), ChunkID{1});

  TableColumnDefinitions column_definitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{20});

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

}  // namespace hyrise
