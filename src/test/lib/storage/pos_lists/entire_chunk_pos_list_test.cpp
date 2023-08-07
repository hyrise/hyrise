#include "base_test.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"

namespace hyrise {

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
  auto table = load_table("resources/test_data/tbl/float_int.tbl", ChunkOffset{10}, FinalizeLastChunk::No);
  EXPECT_EQ(table->chunk_count(), 1);
  auto table_to_add_name = "test_table_to_add";
  auto table_to_add = load_table("resources/test_data/tbl/float_int.tbl", ChunkOffset{10});
  // Insert Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  Hyrise::get().storage_manager.add_table(table_name, table);
  Hyrise::get().storage_manager.add_table(table_to_add_name, table_to_add);

  auto get_table = std::make_shared<GetTable>(table_name);
  get_table->execute();
  const auto chunk_id = ChunkID{0};
  const auto chunk_size = get_table->get_output()->get_chunk(chunk_id)->size();
  const auto entire_chunk_pos_list = std::make_shared<const EntireChunkPosList>(chunk_id, chunk_size);

  const auto insert_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
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

  // TODO(anyone): Maybe add a better check than just size, cause the returned iterators should also handle.
  // this case, which we right now don't check.
}

TEST_F(EntireChunkPosListTest, InsertDoesNotAffectIterators) {
  // This checks that the EntireChunkPosList does not change its iterators after rows were added to the table. These
  // added rows should not be contained in the PosList.

  const auto table = Table::create_dummy_table({{"a", DataType::Int, false}});
  table->append({int32_t{1}});
  table->append({int32_t{2}});
  table->append({int32_t{3}});

  // Currently, the EntireChunkPosList is not linked to the table. Therefore, we do not expect changes to the PosList
  // when new tuples are appended to the table. Nevertheless, we added this test to ensure this assumption is still
  // true even when the EntireChunkPosList might later be aware of the referenced table.
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(), 3);

  const auto entire_chunk_pos_list = std::make_shared<const EntireChunkPosList>(ChunkID{0}, ChunkOffset{3});

  table->append({int32_t{4}});
  // One row has been added to the table.
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(), 4);
  // Newly added rows are not in the position list.
  EXPECT_EQ(entire_chunk_pos_list->begin().dereference().chunk_offset, 0);
  EXPECT_EQ(entire_chunk_pos_list->size(), 3);
  EXPECT_EQ(entire_chunk_pos_list->begin().distance_to(entire_chunk_pos_list->end()), 3);
  EXPECT_EQ(entire_chunk_pos_list->cbegin().distance_to(entire_chunk_pos_list->cend()), 3);
}

}  // namespace hyrise
