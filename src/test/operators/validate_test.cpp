#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsValidateTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = load_table("src/test/tables/validate_input.tbl", 2u);
    set_all_records_visible(*_test_table);
    set_record_invisible_for(*_test_table, RowID{ChunkID{1}, 0u}, 2u);

    _table_wrapper = std::make_shared<TableWrapper>(_test_table);

    _table_wrapper->execute();
  }

  void set_all_records_visible(Table& table);
  void set_record_invisible_for(Table& table, RowID row, CommitID end_cid);

  std::shared_ptr<Table> _test_table;
  std::shared_ptr<TableWrapper> _table_wrapper;
};

void OperatorsValidateTest::set_all_records_visible(Table& table) {
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

    for (auto i = 0u; i < chunk->size(); ++i) {
      mvcc_data->begin_cids[i] = 0u;
      mvcc_data->end_cids[i] = MvccData::MAX_COMMIT_ID;
    }
  }
}

void OperatorsValidateTest::set_record_invisible_for(Table& table, RowID row, CommitID end_cid) {
  table.get_chunk(row.chunk_id)->get_scoped_mvcc_data_lock()->end_cids[row.chunk_offset] = end_cid;
}

TEST_F(OperatorsValidateTest, SimpleValidate) {
  auto context = std::make_shared<TransactionContext>(1u, 3u);

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/validate_output_validated.tbl", 2u);

  auto validate = std::make_shared<Validate>(_table_wrapper);
  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result);
}

TEST_F(OperatorsValidateTest, ScanValidate) {
  auto context = std::make_shared<TransactionContext>(1u, 3u);

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/validate_output_validated_scanned.tbl", 2u);

  auto a = PQPColumnExpression::from_table(*_test_table, "a");
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, greater_than_equals_(a, 2));
  table_scan->set_transaction_context(context);
  table_scan->execute();

  auto validate = std::make_shared<Validate>(table_scan);
  validate->set_transaction_context(context);
  validate->execute();

  EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), expected_result);
}

}  // namespace opossum
