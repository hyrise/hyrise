#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

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

  void SetUp() override {}  // managed by each test individually

  void TearDown() override {
    StorageManager::reset();
    PluginManager::reset();
  }

 protected:
  void _increment_all_values_by_one() {
    auto transaction_context = TransactionManager::get().new_transaction_context();
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
    return MvccDeletePlugin::_try_logical_delete(table_name, chunk_id);
  }
  static void _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id) {
    MvccDeletePlugin::_delete_chunk_physically(StorageManager::get().get_table(table_name), chunk_id);
  }
  static int _get_int_value_from_table(const std::shared_ptr<const Table>& table, const ChunkID chunk_id,
                                       const ColumnID column_id, const ChunkOffset chunk_offset) {
    const auto& segment = table->get_chunk(chunk_id)->get_segment(column_id);
    const auto& value_alltype = static_cast<const AllTypeVariant&>((*segment)[chunk_offset]);
    return boost::lexical_cast<int>(value_alltype);
  }

  std::string _table_name{"mvccTestTable"};
  inline static std::shared_ptr<AbstractExpression> _column_a;
};

TEST_F(MvccDeletePluginTest, LoadUnloadPlugin) {
  auto& pm = PluginManager::get();
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
  const size_t chunk_size = 5;

  // Prepare test
  const auto table = load_table("resources/test_data/tbl/int3.tbl", chunk_size);
  StorageManager::get().add_table(_table_name, table);
  // --- Check table structure
  // --- Expected: 1, 2, 3
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(), 3);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{0}), 1);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{1}), 2);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{2}), 3);
  // --- Invalidate records
  _increment_all_values_by_one();

  // Check pre-conditions
  // --- Check table structure (underscores represent invalidated records)
  // --- Expected: _, _, _, 2, 3 | 4
  EXPECT_EQ(table->chunk_count(), 2);
  EXPECT_EQ(table->row_count(), 6);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{3}), 2);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{4}), 3);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{0}), 4);
  EXPECT_FALSE(table->get_chunk(ChunkID{0})->get_cleanup_commit_id());

  // Delete chunk logically
  EXPECT_TRUE(_try_logical_delete(_table_name, ChunkID{0}));

  // Check Post-Conditions
  EXPECT_TRUE(table->get_chunk(ChunkID{0})->get_cleanup_commit_id());
  // --- Check table structure
  // --- Expected: _, _, _, _, _ | 4, 2, 3
  EXPECT_EQ(table->chunk_count(), 2);
  EXPECT_EQ(table->row_count(), 8);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{0}), 4);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{1}), 2);
  EXPECT_EQ(_get_int_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{2}), 3);
  // --- Check count of invalidations
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->set_transaction_context(transaction_context);
  get_table->execute();
  auto validate_table = std::make_shared<Validate>(get_table);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();
  EXPECT_EQ(validate_table->get_output()->row_count(), 3);
  EXPECT_EQ(validate_table->get_output()->chunk_count(), 1);
}

/**
 * This test checks the physical delete of the MvccDeletePlugin. At first,
 * the logical delete is performed as described in the former test. Afterwards,
 * the first chunk has a cleanup_commit_id and can be deleted physically. After
 * the physical delete, the table returns a nullptr when getting the chunk.
 */
TEST_F(MvccDeletePluginTest, PhysicalDelete) {
  const size_t chunk_size = 5;
  ChunkID chunk_to_delete_id{0};

  // Prepare the test
  const auto& table = load_table("resources/test_data/tbl/int3.tbl", chunk_size);
  const auto& chunk = table->get_chunk(chunk_to_delete_id);
  StorageManager::get().add_table(_table_name, table);
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
