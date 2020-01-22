#include <memory>
#include <string>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "operators/import.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsImportTest : public BaseTest {
 protected:
  const std::string reference_filepath = "resources/test_data/";
  const std::map<FileType, std::string> reference_filenames{
      {FileType::Binary, "bin/float"}, {FileType::Tbl, "tbl/float"}, {FileType::Csv, "csv/float"}};
  const std::map<FileType, std::string> file_extensions{
      {FileType::Binary, ".bin"}, {FileType::Tbl, ".tbl"}, {FileType::Csv, ".csv"}};
};

class OperatorsImportMultiFileTypeTest : public OperatorsImportTest, public ::testing::WithParamInterface<FileType> {};

auto formatter = [](const ::testing::TestParamInfo<FileType> info) {
  auto stream = std::stringstream{};
  stream << info.param;

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(FileTypes, OperatorsImportMultiFileTypeTest,
                         ::testing::Values(FileType::Csv, FileType::Tbl, FileType::Binary), formatter);

TEST_P(OperatorsImportMultiFileTypeTest, ImportWithFileType) {
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, 5);
  expected_table->append({1.1f});
  expected_table->append({2.2f});
  expected_table->append({3.3f});
  expected_table->append({4.4f});

  std::string reference_filename = reference_filepath + reference_filenames.at(GetParam());
  auto importer = std::make_shared<opossum::Import>(reference_filename, "a", Chunk::DEFAULT_SIZE, GetParam());
  importer->execute();
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

TEST_P(OperatorsImportMultiFileTypeTest, ImportWithoutFileType) {
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, 5);
  expected_table->append({1.1f});
  expected_table->append({2.2f});
  expected_table->append({3.3f});
  expected_table->append({4.4f});

  std::string reference_filename =
      reference_filepath + reference_filenames.at(GetParam()) + file_extensions.at(GetParam());
  auto importer = std::make_shared<opossum::Import>(reference_filename, "a");
  importer->execute();
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
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
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->max_chunk_size(), 20U);

  // check if actual chunk_size is correct
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->get_chunk(ChunkID{0})->size(), 20U);
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->get_chunk(ChunkID{1})->size(), 20U);
}

TEST_F(OperatorsImportTest, MaxChunkSize) {
  auto importer =
      std::make_shared<Import>("resources/test_data/csv/float_int_large_chunksize_max.csv", "a", Chunk::DEFAULT_SIZE);
  importer->execute();

  // check if chunk_size property is correct (maximum chunk size)
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->max_chunk_size(), Chunk::DEFAULT_SIZE);

  // check if actual chunk_size and chunk_count is correct
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->get_chunk(ChunkID{0})->size(), 100U);
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("a")->chunk_count(), ChunkID{1});

  TableColumnDefinitions column_definitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("a"), expected_table);
}

}  // namespace opossum
