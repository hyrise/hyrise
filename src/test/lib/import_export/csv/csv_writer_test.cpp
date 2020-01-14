#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

class CsvWriterTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::String, false);
    column_definitions.emplace_back("c", DataType::Float, false);

    table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
  }

  void TearDown() override {
    std::remove(test_filename.c_str());
    std::remove(test_meta_filename.c_str());
  }

  bool file_exists(const std::string& name) {
    std::ifstream file{name};
    return file.good();
  }

  bool compare_file(const std::string& filename, const std::string& expected_content) {
    std::ifstream file(filename);
    Assert(file.is_open(), "compare_file: Could not find file " + filename);

    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    int equality = content.compare(expected_content);
    if (equality != 0) {
      std::cout << equality << std::endl;
      std::cout << "Comparison of file to expected content failed. " << std::endl;
      std::cout << "Expected:" << std::endl;
      std::cout << expected_content << std::endl;
      std::cout << "Actual:" << std::endl;
      std::cout << content << std::endl;
    }
    return equality == 0;
  }

  std::shared_ptr<Table> table;
  const std::string test_filename = test_data_path + "export_test.csv";
  const std::string test_meta_filename = test_filename + CsvMeta::META_FILE_EXTENSION;
};

TEST_F(CsvWriterTest, SingleChunkAndMetaInfo) {
  table->append({1, "Hallo", 3.5f});
  CsvWriter::write(*table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename, "1,\"Hallo\",3.5\n"));
}

TEST_F(CsvWriterTest, EscapeString) {
  table->append({1, "Sie sagte: \"Mir geht's gut, und dir?\"", 3.5f});
  CsvWriter::write(*table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename, "1,\"Sie sagte: \"\"Mir geht's gut, und dir?\"\"\",3.5\n"));
}

TEST_F(CsvWriterTest, MultipleChunks) {
  table->append({1, "Hallo", 3.5f});
  table->append({2, "Welt!", 3.5f});
  table->append({3, "Gute", -4.0f});
  table->append({4, "Nacht", 7.5f});
  table->append({5, "Guten", 8.33f});
  table->append({6, "Tag", 3.5f});
  CsvWriter::write(*table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename,
                           "1,\"Hallo\",3.5\n"
                           "2,\"Welt!\",3.5\n"
                           "3,\"Gute\",-4\n"
                           "4,\"Nacht\",7.5\n"
                           "5,\"Guten\",8.33\n"
                           "6,\"Tag\",3.5\n"));
}

TEST_F(CsvWriterTest, DictionarySegmentFixedSizeByteAligned) {
  table->append({1, "Hallo", 3.5f});
  table->append({1, "Hallo", 3.5f});
  table->append({1, "Hallo3", 3.55f});

  ChunkEncoder::encode_chunks(table, {ChunkID{0}}, EncodingType::Dictionary);
  CsvWriter::write(*table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename,
                           "1,\"Hallo\",3.5\n"
                           "1,\"Hallo\",3.5\n"
                           "1,\"Hallo3\",3.55\n"));
}

TEST_F(CsvWriterTest, FixedStringDictionarySegmentFixedSizeByteAligned) {
  const auto filename_string_table = test_data_path + "string.tbl";
  const auto meta_filename_string_table = filename_string_table + CsvMeta::META_FILE_EXTENSION;

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto string_table = std::make_shared<Table>(column_definitions, TableType::Data, 4);
  string_table->append({"a"});
  string_table->append({"string"});
  string_table->append({"xxx"});
  string_table->append({"www"});
  string_table->append({"yyy"});
  string_table->append({"uuu"});
  string_table->append({"ttt"});
  string_table->append({"zzz"});

  ChunkEncoder::encode_chunks(string_table, {ChunkID{0}}, EncodingType::FixedStringDictionary);
  CsvWriter::write(*string_table, filename_string_table);

  EXPECT_TRUE(file_exists(filename_string_table));
  EXPECT_TRUE(file_exists(meta_filename_string_table));
  EXPECT_TRUE(compare_file(filename_string_table,
                           "\"a\"\n"
                           "\"string\"\n"
                           "\"xxx\"\n"
                           "\"www\"\n"
                           "\"yyy\"\n"
                           "\"uuu\"\n"
                           "\"ttt\"\n"
                           "\"zzz\"\n"));

  std::remove(filename_string_table.c_str());
  std::remove(meta_filename_string_table.c_str());
}

TEST_F(CsvWriterTest, ReferenceSegment) {
  table->append({1, "abc", 1.1f});
  table->append({2, "asdf", 2.2f});
  table->append({3, "hello", 3.3f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto scan = create_table_scan(table_wrapper, ColumnID{0}, PredicateCondition::LessThan, 5);
  scan->execute();
  CsvWriter::write(*(scan->get_output()), test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename,
                           "1,\"abc\",1.1\n"
                           "2,\"asdf\",2.2\n"
                           "3,\"hello\",3.3\n"));
}

TEST_F(CsvWriterTest, ExportAllTypes) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::String, false);
  column_definitions.emplace_back("c", DataType::Float, false);
  column_definitions.emplace_back("d", DataType::Long, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  std::shared_ptr<Table> new_table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
  new_table->append({1, "Hallo", 3.5f, static_cast<int64_t>(12), 2.333});
  CsvWriter::write(*new_table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename, "1,\"Hallo\",3.5,12,2.333\n"));
}

TEST_F(CsvWriterTest, NonsensePath) {
  table->append({1, "hello", 3.5f});
  EXPECT_THROW(CsvWriter::write(*table, "this/path/does/not/exist"), std::exception);
}

TEST_F(CsvWriterTest, ExportNumericNullValues) {
  auto new_table = load_table("resources/test_data/tbl/int_float_with_null.tbl", 4);
  CsvWriter::write(*new_table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename,
                           "12345,458.7\n"
                           "123,\n"
                           ",456.7\n"
                           "1234,457.7\n"));
}

TEST_F(CsvWriterTest, ExportStringNullValues) {
  auto new_table = load_table("resources/test_data/tbl/string_with_null.tbl", 4);
  CsvWriter::write(*new_table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));
  EXPECT_TRUE(compare_file(test_filename,
                           "\"xxx\"\n"
                           "\"www\"\n"
                           "\n"
                           "\"zzz\"\n"));
}

TEST_F(CsvWriterTest, ExportNullValuesMeta) {
  auto new_table = load_table("resources/test_data/tbl/int_float_with_null.tbl", 4);
  CsvWriter::write(*new_table, test_filename);

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(file_exists(test_meta_filename));

  auto meta_information = process_csv_meta_file(test_meta_filename);
  EXPECT_TRUE(meta_information.columns.at(0).nullable);
  EXPECT_TRUE(meta_information.columns.at(1).nullable);
}

}  // namespace opossum
