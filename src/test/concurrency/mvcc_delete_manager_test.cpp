#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base_test.hpp"
#include "concurrency/mvcc_delete_manager.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "gtest/gtest.h"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class MvccDeleteManagerTest : public BaseTest {
 public:
  static void SetUpTestCase() { _column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a"); }

  void SetUp() override {}  // managed by each test individually

  void TearDown() override { StorageManager::reset(); }

  void _incrementAllValuesByOne() {
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

 protected:
  std::string _table_name{"mvccTestTable"};
  inline static std::shared_ptr<AbstractExpression> _column_a;
  static int _get_value_from_table(const std::shared_ptr<const Table> table, const ChunkID chunk_id,
                                   const ColumnID column_id, const ChunkOffset chunk_offset) {
    const auto segment = table->get_chunk(chunk_id)->get_segment(column_id);
    const auto& value_alltype = static_cast<const AllTypeVariant&>((*segment)[chunk_offset]);
    return boost::lexical_cast<int>(value_alltype);
  }
};

TEST_F(MvccDeleteManagerTest, LogicalDelete) {
  const size_t chunk_size = 5;

  // Prepare test
  auto& sm = StorageManager::get();
  const auto table = load_table("src/test/tables/int3.tbl", chunk_size);
  sm.add_table(_table_name, table);

  // Check table structure
  // Expected: 1, 2, 3
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(), 3);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{0}), 1);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{1}), 2);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{2}), 3);

  _incrementAllValuesByOne();

  // Check table structure (underscores represent invalidated records)
  // Expected: _, _, _, 2, 3 | 4
  EXPECT_EQ(sm.get_table(_table_name)->chunk_count(), 2);
  EXPECT_EQ(sm.get_table(_table_name)->row_count(), 6);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{3}), 2);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{0}, ColumnID{0}, ChunkOffset{4}), 3);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{0}), 4);

  // Delete chunk logically
  MvccDeleteManager::run_logical_delete(_table_name, ChunkID{0});

  // Check table structure
  // Expected: _, _, _, _, _ | 4, 2, 3
  EXPECT_EQ(sm.get_table(_table_name)->chunk_count(), 2);
  EXPECT_EQ(sm.get_table(_table_name)->row_count(), 8);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{0}), 4);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{1}), 2);
  EXPECT_EQ(_get_value_from_table(table, ChunkID{1}, ColumnID{0}, ChunkOffset{2}), 3);

  // Check invalidations
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->set_transaction_context(transaction_context);
  get_table->execute();
  auto validate_table = std::make_shared<Validate>(get_table);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();
  EXPECT_EQ(validate_table->get_output()->row_count(), 3);
}

}  // namespace opossum