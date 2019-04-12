#include <chrono>
#include <numeric>
#include <thread>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/plugin_test_utils.hpp"

using namespace opossum;  // NOLINT

class MvccDeletePluginSystemTest : public BaseTest {
 public:
  static void SetUpTestCase() {}

  // Create table with one segment with increasing values from 0 to MAX_CHUNK_SIZE.
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("number", data_type_from_type<int>());

    std::vector<int> vec(MAX_CHUNK_SIZE);
    std::iota(vec.begin(), vec.end(), 0);

    Segments segments;
    const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(vec));
    segments.emplace_back(value_segment);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, MAX_CHUNK_SIZE, UseMvcc::Yes);
    table->append_chunk(segments);

    StorageManager::get().add_table("mvcc_test", table);
  }  // managed by each test individually

  void TearDown() override {
    StorageManager::get().drop_table("mvcc_test");
    StorageManager::reset();
  }

 protected:
  // Update all values in the table to create invalidated rows.
  void update_table() {
    auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

    auto& tm = TransactionManager::get();

    const auto transaction_context = tm.new_transaction_context();
    const auto value = static_cast<int>(_counter++ % (MAX_CHUNK_SIZE));
    const auto expr = expression_functional::equals_(column, value);

    const auto gt = std::make_shared<GetTable>("mvcc_test");
    gt->set_transaction_context(transaction_context);

    const auto validate = std::make_shared<Validate>(gt);
    validate->set_transaction_context(transaction_context);

    const auto where = std::make_shared<TableScan>(validate, expr);
    where->set_transaction_context(transaction_context);

    const auto update = std::make_shared<Update>("mvcc_test", where, where);
    update->set_transaction_context(transaction_context);

    gt->execute();
    validate->execute();
    where->execute();
    update->execute();

    if (!update->execute_failed()) {
      transaction_context->commit();
    } else {
      transaction_context->rollback();
      _counter--;
    }
  }

  constexpr static size_t MAX_CHUNK_SIZE = 200;

  constexpr static size_t PHYSICALLY_DELETED_CHUNKS_COUNT = 3;
  constexpr static size_t MAX_ROWS = MAX_CHUNK_SIZE * 400;
  constexpr static size_t LOOP_COUNT = 10;

  size_t _deleted_chunks = 0;
  size_t _counter = 0;
  size_t _rows = 0;
};

/**
 * This test checks the number of physically deleted chunks, which means
 * nullptrs in the table.
 * These nullptrs are created when the plugin successfully deleted a chunk.
 * This test was designed for _IDLE_DELAY_LOGICAL_DELETE = _IDLE_DELAY_PHYSICAL_DELETE = 1000
 * and may fail if these constants are changed in the MvccDeletePlugin.
 */
TEST_F(MvccDeletePluginSystemTest, CheckPlugin) {
  auto& pm = PluginManager::get();
  pm.load_plugin(build_dylib_path("libMvccDeletePlugin"));

  // This thread calls update_table() continuously.
  std::unique_ptr<PausableLoopThread> table_update_thread =
      std::make_unique<PausableLoopThread>(std::chrono::milliseconds(0), [&](size_t) { update_table(); });

  for (size_t loop_count = 0;
       _deleted_chunks < PHYSICALLY_DELETED_CHUNKS_COUNT && (_rows < MAX_ROWS || loop_count < LOOP_COUNT);
       ++loop_count) {
    const auto table = StorageManager::get().get_table("mvcc_test");
    _deleted_chunks = 0;
    _rows = table->row_count();

    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      if (!table->get_chunk(chunk_id)) {
        _deleted_chunks++;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  }

  EXPECT_GT(_deleted_chunks, 0);

  PluginManager::get().unload_plugin("MvccDeletePlugin");
}
