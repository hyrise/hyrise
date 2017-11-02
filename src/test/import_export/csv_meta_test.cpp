#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "import_export/csv_meta.hpp"

namespace opossum {

class CsvMetaTest : public BaseTest {};

TEST_F(CsvMetaTest, ProcessCsvMetaFile) {
  auto meta = process_csv_meta_file("src/test/csv/sample_meta_information.csv.json");

  auto meta_expected = CsvMeta{};
  meta_expected.chunk_size = 2;
  meta_expected.columns.emplace_back(ColumnMeta{"a", "int", false});
  meta_expected.columns.emplace_back(ColumnMeta{"b", "string", false});
  meta_expected.columns.emplace_back(ColumnMeta{"c", "float", true});

  EXPECT_EQ(meta_expected, meta);
}

TEST_F(CsvMetaTest, ProcessCsvMetaFileMissing) {
  EXPECT_THROW(process_csv_meta_file("src/test/import_export/missing_file.csv.json"), std::logic_error);
}

TEST_F(CsvMetaTest, MinimalMetaInformation) {
  auto json_meta = R"(
    {
      "chunk_size": 0
    }
  )"_json;

  auto meta_expected = CsvMeta{};
  meta_expected.chunk_size = 0;

  EXPECT_EQ(meta_expected, static_cast<CsvMeta>(json_meta));
}

TEST_F(CsvMetaTest, JsonSyntaxError) {
  EXPECT_THROW(process_csv_meta_file("src/test/import_export/json_syntax_error.csv.json"), nlohmann::json::exception);
}

TEST_F(CsvMetaTest, MustProvideChunkSize) {
  auto json_meta = R"(
    {}
  )"_json;

  CsvMeta meta;
  EXPECT_THROW(from_json(json_meta, meta), nlohmann::json::exception);
}

TEST_F(CsvMetaTest, ParseConfigOnlySingleCharacters) {
  auto json_meta = R"(
    {
      "chunk_size": 5,
      "columns": [
        {
          "name": "a",
          "type": "string"
        }
      ],
      "config": {
        "delimiter": "\n\n"
      }
    }
  )"_json;

  CsvMeta meta;
  EXPECT_THROW(from_json(json_meta, meta), std::logic_error);
}

TEST_F(CsvMetaTest, ColumnsMustBeArray) {
  auto json_meta = R"(
    {
      "chunk_size": 5,
      "columns": {}
    }
  )"_json;

  CsvMeta meta;
  EXPECT_THROW(from_json(json_meta, meta), std::logic_error);
}

TEST_F(CsvMetaTest, ChunkSizeNotNegative) {
  auto json_meta = R"(
    {
      "chunk_size": -1
    }
  )"_json;

  CsvMeta meta;
  EXPECT_THROW(from_json(json_meta, meta), std::logic_error);
}

TEST_F(CsvMetaTest, ChunkSizeTypeMismatch) {
  auto json_meta = R"(
    {
      "chunk_size": 0.4
    }
  )"_json;

  CsvMeta meta;
  EXPECT_THROW(from_json(json_meta, meta), std::logic_error);
}

}  // namespace opossum
