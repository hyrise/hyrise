#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_context.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/insert.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class OperatorsInsertTest : public BaseTest {
 protected:
  void SetUp() override {
    t = load_table("src/test/tables/float_int.tbl", 0u);
    StorageManager::get().add_table(table_name, t);

    t2 = load_table("src/test/tables/float_int.tbl", 0u);
    StorageManager::get().add_table(table_name2, t2);

    gt = std::make_shared<GetTable>(table_name2);
    gt->execute();
  }

  std::ostringstream output;

  std::string table_name = "insertTestTable";
  std::string table_name2 = "insertTestTable2";

  uint32_t chunk_size = 10;

  std::shared_ptr<Table> t = nullptr;
  std::shared_ptr<GetTable> gt;
  std::shared_ptr<Table> t2 = nullptr;
};

TEST_F(OperatorsInsertTest, SelfInsert) {
  auto ins = std::make_shared<Insert>(table_name, gt);
  auto context = TransactionContext(1, 1);
  ins->execute(&context);

  // Check that row has been inserted.
  EXPECT_EQ(t->get_chunk(0).size(), 6u);
  EXPECT_EQ((*t->get_chunk(0).get_column(1))[0], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(0).get_column(0))[0], AllTypeVariant(458.7f));
  EXPECT_EQ((*t->get_chunk(0).get_column(1))[3], AllTypeVariant(12345));
  EXPECT_EQ((*t->get_chunk(0).get_column(0))[3], AllTypeVariant(458.7f));

  auto output_str = output.str();
}

TEST_F(OperatorsInsertTest, InsertRespectChunkSize) {
  auto t_name = "test1";
  auto t_name2 = "test2";

  // 3 Rows, column_size = 4
  auto t3 = load_table("src/test/tables/int.tbl", 4u);
  StorageManager::get().add_table(t_name, t3);

  // 10 Rows
  auto t4 = load_table("src/test/tables/10_ints.tbl", 0u);
  StorageManager::get().add_table(t_name2, t4);

  auto gt2 = std::make_shared<GetTable>(t_name2);
  gt2->execute();

  auto ins = std::make_shared<Insert>(t_name, gt2);
  auto context = TransactionContext(1, 1);
  ins->execute(&context);

  // Check that we now have 4 chunks with the last one containing one row
  EXPECT_EQ(t3->chunk_count(), 4u);
  EXPECT_EQ(t3->get_chunk(3).size(), 1u);
}

}  // namespace opossum
