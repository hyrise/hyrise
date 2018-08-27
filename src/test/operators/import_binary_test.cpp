#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/import_binary.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class OperatorsImportBinaryTest : public BaseTest {};

TEST_F(OperatorsImportBinaryTest, SingleChunkSingleFloatCxlumn) {
  auto expected_table = std::make_shared<Table>(TableCxlumnDefinitions{{"a", DataType::Float}}, TableType::Data, 5);
  expected_table->append({5.5f});
  expected_table->append({13.0f});
  expected_table->append({16.2f});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/SingleChunkSingleFloatCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, MultipleChunkSingleFloatCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::Float);
  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2);
  expected_table->append({5.5f});
  expected_table->append({13.0f});
  expected_table->append({16.2f});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/MultipleChunkSingleFloatCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
  EXPECT_EQ(importer->get_output()->chunk_count(), 2u);
}

TEST_F(OperatorsImportBinaryTest, StringValueCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);
  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 5);
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/StringValueCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, StringDictionaryCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);
  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 10, UseMvcc::Yes);
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  ChunkEncoder::encode_all_chunks(expected_table);

  StorageManager::get().add_table("table_a", expected_table);

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/StringDictionaryCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, AllTypesValueCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);
  cxlumn_definitions.emplace_back("b", DataType::Int);
  cxlumn_definitions.emplace_back("c", DataType::Long);
  cxlumn_definitions.emplace_back("d", DataType::Float);
  cxlumn_definitions.emplace_back("e", DataType::Double);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/AllTypesValueCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, AllTypesDictionaryCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);
  cxlumn_definitions.emplace_back("b", DataType::Int);
  cxlumn_definitions.emplace_back("c", DataType::Long);
  cxlumn_definitions.emplace_back("d", DataType::Float);
  cxlumn_definitions.emplace_back("e", DataType::Double);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  ChunkEncoder::encode_all_chunks(expected_table);

  StorageManager::get().add_table("expected_table", expected_table);

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/AllTypesDictionaryCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, AllTypesMixCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);
  cxlumn_definitions.emplace_back("b", DataType::Int);
  cxlumn_definitions.emplace_back("c", DataType::Long);
  cxlumn_definitions.emplace_back("d", DataType::Float);
  cxlumn_definitions.emplace_back("e", DataType::Double);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 2, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  ChunkEncoder::encode_chunks(expected_table, {ChunkID{0}});

  StorageManager::get().add_table("expected_table", expected_table);

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/AllTypesMixCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, FileDoesNotExist) {
#ifdef __SANITIZE_ADDRESS__
  // This test appears to cause stack corruption when the following come together:
  // gtest, gcc 6.3.0, asan, osx
  // This appears to be related to throwing exceptions with ifstream::exceptions and can be reproduced
  // even outside of the Opossum code.
  return;
#endif

  auto importer = std::make_shared<opossum::ImportBinary>("not_existing_file");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportBinaryTest, TwoCxlumnsNoValues) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("FirstCxlumn", DataType::Int);
  cxlumn_definitions.emplace_back("SecondCxlumn", DataType::String);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 30'000);

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/TwoCxlumnsNoValues.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, EmptyStringsValueCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 10);

  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/EmptyStringsValueCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, EmptyStringsDictionaryCxlumn) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::String);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data, 10);

  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/EmptyStringsDictionaryCxlumn.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, SaveToStorageManager) {
  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/float.bin", std::string("float_table"));
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
  EXPECT_TABLE_EQ_ORDERED(StorageManager::get().get_table("float_table"), expected_table);
}

TEST_F(OperatorsImportBinaryTest, FallbackToRetrieveFromStorageManager) {
  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/float.bin", std::string("float_table"));
  importer->execute();
  auto retriever =
      std::make_shared<opossum::ImportBinary>("src/test/binary/AllTypesMixCxlumn.bin", std::string("float_table"));
  retriever->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), retriever->get_output());
  EXPECT_TABLE_EQ_ORDERED(StorageManager::get().get_table("float_table"), retriever->get_output());
}

TEST_F(OperatorsImportBinaryTest, InvalidCxlumnType) {
  auto importer =
      std::make_shared<opossum::ImportBinary>("src/test/binary/InvalidCxlumnType.bin", std::string("float_table"));
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportBinaryTest, InvalidAttributeVectorWidth) {
  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/InvalidAttributeVectorWidth.bin",
                                                          std::string("float_table"));
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportBinaryTest, AllTypesNullValues) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::Int, true);
  cxlumn_definitions.emplace_back("b", DataType::Float, true);
  cxlumn_definitions.emplace_back("c", DataType::Long, true);
  cxlumn_definitions.emplace_back("d", DataType::String, true);
  cxlumn_definitions.emplace_back("e", DataType::Double, true);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data);

  expected_table->append({opossum::NULL_VALUE, 1.1f, 100, "one", 1.11});
  expected_table->append({2, opossum::NULL_VALUE, 200, "two", 2.22});
  expected_table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  expected_table->append({4, 4.4f, 400, opossum::NULL_VALUE, 4.44});
  expected_table->append({5, 5.5f, 500, "five", opossum::NULL_VALUE});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/AllTypesNullValues.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportBinaryTest, AllTypesDictionaryNullValues) {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("a", DataType::Int, true);
  cxlumn_definitions.emplace_back("b", DataType::Float, true);
  cxlumn_definitions.emplace_back("c", DataType::Long, true);
  cxlumn_definitions.emplace_back("d", DataType::String, true);
  cxlumn_definitions.emplace_back("e", DataType::Double, true);

  auto expected_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data);

  expected_table->append({opossum::NULL_VALUE, 1.1f, 100, "one", 1.11});
  expected_table->append({2, opossum::NULL_VALUE, 200, "two", 2.22});
  expected_table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  expected_table->append({4, 4.4f, 400, opossum::NULL_VALUE, 4.44});
  expected_table->append({5, 5.5f, 500, "five", opossum::NULL_VALUE});

  auto importer = std::make_shared<opossum::ImportBinary>("src/test/binary/AllTypesDictionaryNullValues.bin");
  importer->execute();

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

}  // namespace opossum
