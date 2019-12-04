#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "operators/import.hpp"
#include "storage/table.hpp"

#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

class OperatorsImportTest : public BaseTest {};

TEST_F(OperatorsImportTest, FileDoesNotExist) {
  auto importer = std::make_shared<Import>("not_existing_file");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportTest, UnknownFileExtension) {
  auto importer = std::make_shared<Import>("not_existing_file.mp3");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportTest, SaveToStorageManager) {
  auto importer = std::make_shared<Import>("resources/test_data/csv/float.csv", std::string("float_table"));
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("float_table"), expected_table);
}

TEST_F(OperatorsImportTest, FallbackToRetrieveFromStorageManager) {
  auto importer = std::make_shared<Import>("resources/test_data/csv/float.csv", std::string("float_table"));
  importer->execute();
  auto retriever = std::make_shared<Import>("resources/test_data/csv/float_int.csv", std::string("float_table"));
  retriever->execute();
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), retriever->get_output());
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("float_table"), retriever->get_output());
}

TEST_F(OperatorsImportTest, Parallel) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  auto importer = std::make_shared<OperatorTask>(
      std::make_shared<Import>("resources/test_data/csv/float_int_large.csv"), CleanupTemporaries::Yes);
  importer->schedule();

  TableColumnDefinitions column_definitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  Hyrise::get().scheduler()->finish();
  EXPECT_TABLE_EQ_ORDERED(importer->get_operator()->get_output(), expected_table);
  Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
}

TEST_F(OperatorsImportTest, ChunkSize) {
  auto importer = std::make_shared<Import>("resources/test_data/csv/float_int_large.csv", std::nullopt, std::nullopt,
                                           ChunkOffset{20});
  importer->execute();

  // check if chunk_size property is correct
  EXPECT_EQ(importer->get_output()->max_chunk_size(), 20U);

  // check if actual chunk_size is correct
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{0})->size(), 20U);
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{1})->size(), 20U);
}

TEST_F(OperatorsImportTest, MaxChunkSize) {
  auto importer = std::make_shared<Import>("resources/test_data/csv/float_int_large_chunksize_max.csv", std::nullopt,
                                           std::nullopt, Chunk::DEFAULT_SIZE);
  importer->execute();

  // check if chunk_size property is correct (maximum chunk size)
  EXPECT_EQ(importer->get_output()->max_chunk_size(), Chunk::DEFAULT_SIZE);

  // check if actual chunk_size and chunk_count is correct
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{0})->size(), 100U);
  EXPECT_EQ(importer->get_output()->chunk_count(), ChunkID{1});

  TableColumnDefinitions column_definitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}
}  // namespace opossum
