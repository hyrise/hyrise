#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "../../plugins/mvcc_delete_plugin.hpp"
#include "../utils/plugin_test_utils.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class MvccDeletePluginTest : public BaseTest {
 public:
  static void SetUpTestCase() { _column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a"); }

  void SetUp() override {
    const auto& table = load_table("resources/test_data/tbl/int3.tbl", _chunk_size);
    Hyrise::get().storage_manager.add_table(_table_name, table);
  }

  void TearDown() override { Hyrise::reset(); }

 protected:
  void _increment_all_values_by_one() {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
    // GetTable
    auto get_table = std::make_shared<GetTable>(_table_name);
    get_table->set_transaction_context(transaction_context);
    get_table->execute();

    // Validate
    auto validate_table = std::make_shared<Validate>(get_table);
    validate_table->set_transaction_context(transaction_context);
    validate_table->execute();

    // Update
    auto update_expressions = expression_vector(add_(_column_a, 1));
    auto updated_values_projection = std::make_shared<Projection>(validate_table, update_expressions);
    updated_values_projection->execute();
    auto update_table = std::make_shared<Update>(_table_name, validate_table, updated_values_projection);
    update_table->set_transaction_context(transaction_context);
    update_table->execute();

    transaction_context->commit();
  }
  static bool _try_logical_delete(const std::string& table_name, ChunkID chunk_id) {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
    return MvccDeletePlugin::_try_logical_delete(table_name, chunk_id, transaction_context);
  }
  static bool _try_logical_delete(const std::string& table_name, ChunkID chunk_id,
                                  std::shared_ptr<TransactionContext> transaction_context) {
    return MvccDeletePlugin::_try_logical_delete(table_name, chunk_id, transaction_context);
  }
  static void _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id) {
    MvccDeletePlugin::_delete_chunk_physically(Hyrise::get().storage_manager.get_table(table_name), chunk_id);
  }

  static int _get_int_value_from_table(const std::shared_ptr<const Table>& table, const ChunkID chunk_id,
                                       const ColumnID column_id, const ChunkOffset chunk_offset) {
    const auto& segment = table->get_chunk(chunk_id)->get_segment(column_id);
    const auto& value_alltype = static_cast<const AllTypeVariant&>((*segment)[chunk_offset]);
    return boost::lexical_cast<int>(value_alltype);
  }

  const std::string _table_name{"mvccTestTable"};
  static constexpr auto _chunk_size = size_t{4};
  inline static std::shared_ptr<AbstractExpression> _column_a;
};

TEST_F(MvccDeletePluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libMvccDeletePlugin"));
  pm.unload_plugin("MvccDeletePlugin");
}

/**
 * This test checks the logical delete. All values in the table are incremented to
 * generate three invalidated rows and create a second chunk. Before the logical delete
 * is performed, the first chunk contains a mix of valid and invalidated lines. After
 * the logical delete, all its rows are invalidated and a cleanup_commit_id was set,
 * which is used for the physical delete. All values are now located in the
 * second chunk. When fetching the table, the fully invalidated chunk is not
 * visible anymore for transactions.
 */
TEST_F(MvccDeletePluginTest, LogicalDelete) {
  const auto table = Hyrise::get().storage_manager.get_table(_table_name);

  // Prepare test
  // --- Check table structure
  // --- Expected: 1, 2, 3 |
  // --- Chunk 0 is already immutable due to load_table()
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(), 3);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{0}), 1);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{1}), 2);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{2}), 3);

  // --- Invalidate records – so that chunk 0 is completely invalidated
  _increment_all_values_by_one();
  // --- Check table structure (underscores represent invalidated records)
  // --- Expected: _, _, _ | 2, 3, 4
  EXPECT_EQ(table->chunk_count(), 2);
  EXPECT_EQ(table->row_count(), 6);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{0}), 2);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{1}), 3);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{2}), 4);

  // --- Invalidate records - so that chunk 1 is completely invalidated except for one record
  _increment_all_values_by_one();
  // --- Expected: _, _, _ | _, _, _, 3 | 4, 5
  EXPECT_EQ(table->chunk_count(), 3);
  EXPECT_EQ(table->row_count(), 9);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{3}), 3);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{2}, ColumnID{0}, ChunkOffset{0}), 4);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{2}, ColumnID{0}, ChunkOffset{1}), 5);

  // There should be no cleanup-commit-id set yet
  EXPECT_FALSE(table->get_chunk(ChunkID{0})->get_cleanup_commit_id());
  EXPECT_FALSE(table->get_chunk(ChunkID{1})->get_cleanup_commit_id());
  EXPECT_FALSE(table->get_chunk(ChunkID{2})->get_cleanup_commit_id());

  // Delete chunk 0 logically
  EXPECT_TRUE(_try_logical_delete(_table_name, ChunkID{0}));
  EXPECT_TRUE(table->get_chunk(ChunkID{0})->get_cleanup_commit_id());

  // Delete chunk 1 logically
  EXPECT_TRUE(_try_logical_delete(_table_name, ChunkID{1}));
  EXPECT_TRUE(table->get_chunk(ChunkID{1})->get_cleanup_commit_id());
  // The logical delete of chunk 1 should have changed the table structure
  // --- Expected: _, _, _ | _, _, _, _ | 4, 5, 3
  EXPECT_EQ(table->chunk_count(), 3);
  EXPECT_EQ(table->row_count(), 10);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{2}, ColumnID{0}, ChunkOffset{0}), 4);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{2}, ColumnID{0}, ChunkOffset{1}), 5);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{2}, ColumnID{0}, ChunkOffset{2}), 3);

  // --- Check whether GetTable filters out logically deleted chunks
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->set_transaction_context(transaction_context);
  get_table->execute();
  EXPECT_EQ(get_table->get_output()->chunk_count(), 1);
  EXPECT_EQ(get_table->get_output()->row_count(), 3);
}

TEST_F(MvccDeletePluginTest, LogicalDeleteConflicts) {
  const auto table = Hyrise::get().storage_manager.get_table(_table_name);

  // Prepare test
  _increment_all_values_by_one();
  _increment_all_values_by_one();
  EXPECT_EQ(table->chunk_count(), 3);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->size(), 4);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->invalid_row_count(), 3);

  // Force rollback of logical delete transaction
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();

  {
    const auto conflicting_sql = "DELETE FROM " + _table_name + " WHERE a < 4";
    auto conflicting_sql_pipeline = SQLPipelineBuilder{conflicting_sql}.create_pipeline_statement();
    (void)conflicting_sql_pipeline.get_result_table();
  }

  EXPECT_FALSE(_try_logical_delete(_table_name, ChunkID{1}, transaction_context));
  EXPECT_EQ(transaction_context->phase(), TransactionPhase::RolledBack);
}

/**
 * This test checks the physical delete of the MvccDeletePlugin. At first,
 * the logical delete is performed as described in the former test. Afterwards,
 * the first chunk has a cleanup_commit_id and can be deleted physically. After
 * the physical delete, the table returns a nullptr when getting the chunk.
 */
TEST_F(MvccDeletePluginTest, PhysicalDelete) {
  const auto table = Hyrise::get().storage_manager.get_table(_table_name);

  // Prepare the test
  ChunkID chunk_to_delete_id{0};
  const auto& chunk = table->get_chunk(chunk_to_delete_id);
  // --- invalidate records
  _increment_all_values_by_one();
  // --- delete chunk logically
  EXPECT_FALSE(chunk->get_cleanup_commit_id());
  EXPECT_TRUE(_try_logical_delete(_table_name, chunk_to_delete_id));

  // Run the test
  // --- check pre-conditions
  EXPECT_TRUE(table->get_chunk(ChunkID{0})->get_cleanup_commit_id());

  // --- run physical delete
  _delete_chunk_physically(_table_name, chunk_to_delete_id);

  // --- check post-conditions
  EXPECT_TRUE(table->get_chunk(chunk_to_delete_id) == nullptr);
}

}  // namespace opossum
