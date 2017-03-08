#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsValidateTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table = load_table("src/test/tables/validate_input.tbl", 2u);
    set_all_records_visible(*test_table);
    set_record_invisible_for(*test_table, RowID{1u, 0u}, 2u);

    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<GetTable>("table_a");

    _gt->execute();
  }

  void set_all_records_visible(Table& table);
  void set_record_invisible_for(Table& table, RowID row, CommitID end_cid);

  std::shared_ptr<GetTable> _gt;
};

void OperatorsValidateTest::set_all_records_visible(Table& table) {
  for (auto chunk_id = 0u; chunk_id < table.chunk_count(); ++chunk_id) {
    auto& chunk = table.get_chunk(chunk_id);
    auto& mvcc_columns = chunk.mvcc_columns();

    for (auto i = 0u; i < chunk.size(); ++i) {
      mvcc_columns.begin_cids[i] = 0u;
      mvcc_columns.end_cids[i] = Chunk::MAX_COMMIT_ID;
    }
  }
}

void OperatorsValidateTest::set_record_invisible_for(Table& table, RowID row, CommitID end_cid) {
  table.get_chunk(row.chunk_id).mvcc_columns().end_cids[row.chunk_offset] = end_cid;
}

TEST_F(OperatorsValidateTest, SimpleValidate) {
  auto context = TransactionContext(1u, 3u);

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/validate_output_validated.tbl", 2u);

  auto validate = std::make_shared<Validate>(_gt);
  validate->execute(&context);

  EXPECT_TABLE_EQ(validate->get_output(), expected_result);
}

TEST_F(OperatorsValidateTest, DISABLED_ProjectedValidate) {
  auto context = TransactionContext(1u, 3u);

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/validate_output_validated_projected.tbl", 2u);

  std::vector<std::string> column_filter = {"c", "a"};
  auto projection = std::make_shared<Projection>(_gt, column_filter);
  projection->execute(&context);

  auto validate = std::make_shared<Validate>(projection);
  validate->execute(&context);

  EXPECT_TABLE_EQ(validate->get_output(), expected_result);
}

}  // namespace opossum
