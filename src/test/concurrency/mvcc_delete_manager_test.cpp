#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/load_table.hpp"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/mvcc_delete_manager.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "operators/projection.hpp"
#include "operators/update.hpp"
#include "operators/print.hpp"

namespace opossum {

class MvccDeleteManagerTest : public BaseTest {
public:
    static void SetUpTestCase() {
      _column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a");
    }

    void SetUp() override { }

    void TearDown() override { StorageManager::reset(); }

    void _incrementAllValuesByOne() {
      auto get_table = std::make_shared<GetTable>(_table_name);
      get_table->execute();

      // Validate
      auto transaction_context = TransactionManager::get().new_transaction_context();
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
};


TEST_F(MvccDeleteManagerTest, LogicalDelete) {
  const size_t chunk_size = 5;

  // Prepare
  auto& sm = StorageManager::get();
  const auto table = load_table("src/test/tables/int3.tbl", chunk_size);
  EXPECT_EQ(table->chunk_count(), 1);
  EXPECT_EQ(table->row_count(),3); // 1, 2, 3
  sm.add_table(_table_name, table);

  //_incrementAllValuesByOne();

  EXPECT_EQ(sm.get_table(_table_name)->chunk_count(), 2);
  EXPECT_EQ(sm.get_table(_table_name)->row_count(),6); // _, _, _, 2, 3 | 4

  // Test logical delete
  MvccDeleteManager::run_logical_delete(_table_name, ChunkID{0});
  EXPECT_EQ(sm.get_table(_table_name)->row_count(), 8); // _, _, _, _, _ | 4, 2, 3

  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->execute();
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(get_table);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  EXPECT_EQ(validate_table->get_output()->row_count(), 3);

  auto print_table = std::make_shared<Print>(validate_table, std::cout);
  print_table->execute();
}

}  // namespace opossum