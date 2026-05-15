#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>

#include "magic_enum/magic_enum.hpp"

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "import_export/file_type.hpp"
#include "operators/import.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class OperatorsImportTest : public BaseTest {
 protected:
  const std::string reference_filepath = "resources/test_data/";
  const std::map<FileType, std::string> reference_filenames{
      {FileType::Binary, "bin/int_string2"}, {FileType::Tbl, "tbl/int_string2"}, {FileType::Csv, "csv/int_string2"}};
  const std::map<FileType, std::string> file_extensions{
      {FileType::Binary, ".bin"}, {FileType::Tbl, ".tbl"}, {FileType::Csv, ".csv"}};
};

class OperatorsImportMultiFileTypeAndEncodingTest
    : public OperatorsImportTest,
      public ::testing::WithParamInterface<std::tuple<FileType, std::optional<EncodingType>>> {};

std::string file_types_and_encodings_name_generator(
    const ::testing::TestParamInfo<std::tuple<FileType, std::optional<EncodingType>>>& info) {
  const auto& [file_type, encoding_type] = info.param;

  // Convert FileType and EncodingType to strings for a human readable name.
  const auto encoding_type_str = encoding_type ? std::string{magic_enum::enum_name(*encoding_type)} : "None";

  return std::string{magic_enum::enum_name(file_type)} + "_" + encoding_type_str;
}

// `FixedString` only works on `DataType::String` (`pmr_string`) , `FoR` only on `DataType::Int` (`int32_t`).
INSTANTIATE_TEST_SUITE_P(FileTypesAndEncodings, OperatorsImportMultiFileTypeAndEncodingTest,
                         ::testing::Combine(::testing::Values(FileType::Csv, FileType::Tbl, FileType::Binary),
                                            ::testing::Values(std::nullopt, EncodingType::Unencoded,
                                                              EncodingType::Dictionary, EncodingType::RunLength,
                                                              EncodingType::FixedStringDictionary,
                                                              EncodingType::FrameOfReference, EncodingType::LZ4)),
                         file_types_and_encodings_name_generator);

TEST_P(OperatorsImportMultiFileTypeAndEncodingTest, ImportWithEncodingAndFileType) {
  const auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::String, false}},
                              TableType::Data, ChunkOffset{4});
  expected_table->append({123, "A"});
  expected_table->append({1234, "B"});
  expected_table->append({12345, "C"});

  // The table contains a single chunk. Make it immutable to allow its encoding.
  expected_table->get_chunk(ChunkID{0})->set_immutable();

  const auto& [file_type, encoding] = GetParam();

  // Apply the encoding to the table.
  if (file_type != FileType::Binary) {
    if (encoding) {
      const auto segment_encoding_spec_col1 = SegmentEncodingSpec{
          *encoding != EncodingType::FixedStringDictionary ? *encoding : EncodingType::FrameOfReference};
      const auto segment_encoding_spec_col2 =
          SegmentEncodingSpec{*encoding != EncodingType::FrameOfReference ? *encoding : EncodingType::Dictionary};

      const auto chunk_encoding_spec = ChunkEncodingSpec{segment_encoding_spec_col1, segment_encoding_spec_col2};
      ChunkEncoder::encode_all_chunks(expected_table, chunk_encoding_spec);
    } else {
      // No encoding is specified, go with the default that should be selected by `auto_select_segment_encoding_spec`
      const auto chunk_encoding_spec = ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::FrameOfReference},
                                                         SegmentEncodingSpec{EncodingType::Dictionary}};
      ChunkEncoder::encode_all_chunks(expected_table, chunk_encoding_spec);
    }
  }

  const auto reference_filename =
      reference_filepath + reference_filenames.at(file_type) + file_extensions.at(file_type);
  const auto importer = std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, file_type, encoding);
  importer->execute();
  const auto table = Hyrise::get().storage_manager.get_table("a");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);

  // Check Segment 1 / Column 1.
  const auto segment_1 = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto expected_segment_1 = expected_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});

  // Check if the imported segment has the same type as the manually encoded one.
  resolve_segment_type<int32_t>(*expected_segment_1, [&](const auto& expected_segment) {
    using EncodedSegmentType = std::remove_reference_t<decltype(expected_segment)>;
    EXPECT_TRUE(std::dynamic_pointer_cast<EncodedSegmentType>(segment_1));
  });

  // Check Segment 2 / Column 2.
  const auto segment_2 = table->get_chunk(ChunkID{0})->get_segment(ColumnID{1});
  const auto expected_segment_2 = expected_table->get_chunk(ChunkID{0})->get_segment(ColumnID{1});

  // Check if the imported segment has the same type as the manually encoded one.
  resolve_segment_type<pmr_string>(*expected_segment_2, [&](const auto& expected_segment) {
    using EncodedSegmentType = std::remove_reference_t<decltype(expected_segment)>;
    EXPECT_TRUE(std::dynamic_pointer_cast<EncodedSegmentType>(segment_2));
  });
}

class OperatorsImportFileTypesTest : public OperatorsImportTest, public ::testing::WithParamInterface<FileType> {};

INSTANTIATE_TEST_SUITE_P(FileTypes, OperatorsImportFileTypesTest,
                         ::testing::Values(FileType::Csv, FileType::Tbl, FileType::Binary), enum_formatter<FileType>);

TEST_P(OperatorsImportFileTypesTest, ImportWithAutoFileType) {
  const auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::String, false}},
                              TableType::Data, ChunkOffset{4});
  expected_table->append({123, "A"});
  expected_table->append({1234, "B"});
  expected_table->append({12345, "C"});

  // The table contains a single chunk. Make it immutable to allow its encoding.
  expected_table->get_chunk(ChunkID{0})->set_immutable();

  const auto& file_type = GetParam();

  // The `.bin` file we load is stored and therefore expected to be `Unencoded`.
  if (file_type != FileType::Binary) {
    const auto chunk_encoding_spec = ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::FrameOfReference},
                                                       SegmentEncodingSpec{EncodingType::Dictionary}};
    ChunkEncoder::encode_all_chunks(expected_table, chunk_encoding_spec);
  }

  const auto reference_filename =
      reference_filepath + reference_filenames.at(file_type) + file_extensions.at(file_type);

  // Instead of using the provided `file_type` for the Importer we use the default `FileType::Auto` here.
  const auto importer =
      std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, FileType::Auto, std::nullopt);
  importer->execute();
  const auto table = Hyrise::get().storage_manager.get_table("a");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);

  // Check Segment 1.
  const auto segment_1 = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto expected_segment_1 = expected_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});

  // Check if the imported segment has the same type as the manually encoded one.
  resolve_segment_type<int32_t>(*expected_segment_1, [&](const auto& expected_segment) {
    using EncodedSegmentType = std::remove_reference_t<decltype(expected_segment)>;
    EXPECT_TRUE(std::dynamic_pointer_cast<EncodedSegmentType>(segment_1));
  });

  // Check Segment 2.
  const auto segment_2 = table->get_chunk(ChunkID{0})->get_segment(ColumnID{1});
  const auto expected_segment_2 = expected_table->get_chunk(ChunkID{0})->get_segment(ColumnID{1});

  // Check if the imported segment has the same type as the manually encoded one.
  resolve_segment_type<pmr_string>(*expected_segment_2, [&](const auto& expected_segment) {
    using EncodedSegmentType = std::remove_reference_t<decltype(expected_segment)>;
    EXPECT_TRUE(std::dynamic_pointer_cast<EncodedSegmentType>(segment_2));
  });
}

TEST_P(OperatorsImportMultiFileTypeAndEncodingTest, HasCorrectMvccData) {
  const auto& [file_type, encoding] = GetParam();

  const auto reference_filename =
      reference_filepath + reference_filenames.at(file_type) + file_extensions.at(file_type);
  auto importer = std::make_shared<Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, file_type, encoding);
  importer->execute();

  const auto table = Hyrise::get().storage_manager.get_table("a");

  EXPECT_EQ(table->uses_mvcc(), UseMvcc::Yes);
  EXPECT_TRUE(table->get_chunk(ChunkID{0})->has_mvcc_data());
  EXPECT_EQ(table->get_chunk(ChunkID{0})->mvcc_data()->max_begin_cid.load(), CommitID{0});
}

TEST_F(OperatorsImportTest, FileDoesNotExist) {
  auto importer = std::make_shared<Import>("not_existing_file.tbl", "a");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportTest, UnknownFileExtension) {
  EXPECT_THROW(std::make_shared<Import>("not_existing_file.mp3", "a"), std::exception);
}

TEST_F(OperatorsImportTest, RetrieveCsvMetaFromEmptyTable) {
  auto existing_table = CsvParser::create_table_from_meta_file("resources/test_data/csv/float.csv.json");

  Hyrise::get().storage_manager.add_table("a", existing_table);

  auto expected_table = load_table("resources/test_data/tbl/float.tbl");
  auto importer = std::make_shared<Import>("resources/test_data/csv/float.csv", "a");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

TEST_F(OperatorsImportTest, AppendToExistingTable) {
  auto existing_table = load_table("resources/test_data/tbl/float.tbl");

  Hyrise::get().storage_manager.add_table("a", existing_table);

  auto expected_table = load_table("resources/test_data/tbl/float.tbl");
  Assert(expected_table->chunk_count() == 1, "Testing code was only written to support single chunk tables");
  const auto chunk = expected_table->get_chunk(ChunkID{0});
  expected_table->append_chunk(chunk->segments(), chunk->mvcc_data());

  auto importer = std::make_shared<Import>("resources/test_data/csv/float.csv", "a");
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
