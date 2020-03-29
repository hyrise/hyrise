#include "base_test.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/pos_lists/rowid_pos_list.hpp"

namespace opossum {

class EntireChunkPosListTest : public BaseTest {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);

    dummy_table_wrapper = std::make_shared<TableWrapper>(Table::create_dummy_table(column_definitions));
    dummy_table_wrapper->execute();

    create_table = std::make_shared<CreateTable>("t", false, dummy_table_wrapper);
  }

  TableColumnDefinitions column_definitions;
  std::shared_ptr<TableWrapper> dummy_table_wrapper;
  std::shared_ptr<CreateTable> create_table;
};

TEST_F(EntireChunkPosListTest, AddAfterMatchedAllTest) {
  // This checks if the EntireChunkPosList correctly handles rows that are added to the table
  // after the PosList was created. These later added rows should not be contained in the PosList

  auto table_name = "test_table";
  auto table = load_table("resources/test_data/tbl/float_int.tbl", 10, FinalizeLastChunk::No);
  EXPECT_EQ(table->chunk_count(), 1);
  auto table_to_add_name = "test_table_to_add";
  auto table_to_add = load_table("resources/test_data/tbl/float_int.tbl", 10);
  // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  Hyrise::get().storage_manager.add_table(table_name, table);
  Hyrise::get().storage_manager.add_table(table_to_add_name, table_to_add);

  auto get_table = std::make_shared<GetTable>(table_name);
  get_table->execute();
  const auto chunk_id = ChunkID{0};
  const auto chunk_size = get_table->get_output()->get_chunk(chunk_id)->size();
  const auto entire_chunk_pos_list = std::make_shared<const EntireChunkPosList>(chunk_id, chunk_size);

  const auto insert_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto get_table_to_add = std::make_shared<GetTable>(table_to_add_name);
  get_table_to_add->execute();

  auto insert = std::make_shared<Insert>(table_name, get_table_to_add);
  insert->set_transaction_context(insert_context);
  insert->execute();
  insert_context->commit();

  // Extra Lines have been added to the table:
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(), 6);
  // Newly added rows are not in the position list
  EXPECT_EQ(entire_chunk_pos_list->size(), 3);

  // TODO(XPERIANER): Maybe add a better check than just size, cause the returned iterators should also handle
  // this case, which we right now don't check.
}
}  // namespace opossum
