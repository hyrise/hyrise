#include <iostream>
#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsTableScanTest : public ::testing::Test {
  virtual void SetUp() {
    _test_table = std::make_shared<opossum::Table>(opossum::Table(2));

    _test_table->add_column("a", "int");
    _test_table->add_column("b", "float");

    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});
    _test_table->append({12345, 458.7f});

    opossum::StorageManager::get().add_table("table_a", std::move(_test_table));

    _gt = std::make_shared<opossum::GetTable>("table_a");
  }

  virtual void TearDown() { opossum::StorageManager::get().drop_table("table_a"); }

 public:
  std::shared_ptr<opossum::Table> _test_table;
  std::shared_ptr<opossum::GetTable> _gt;
};

TEST_F(OperatorsTableScanTest, DoubleScan) {
  auto scan_1 = std::make_shared<opossum::TableScan>(_gt, "a", ">=", 1234);
  scan_1->execute();

  auto scan_2 = std::make_shared<opossum::TableScan>(scan_1, "b", "<", 457.9);
  scan_2->execute();

  EXPECT_EQ(type_cast<int>((*(scan_2->get_output()->get_chunk(0).get_column(0)))[0]), 1234);

  EXPECT_EQ(scan_2->get_output()->row_count(), (u_int)1);
}

class OperatorsTableScanImplTest : public ::testing::Test {
  virtual void SetUp() {
    _test_table = std::make_shared<opossum::Table>(opossum::Table(2));

    _test_table->add_column("a", "int");
    _test_table->add_column("b", "float");

    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});
    _test_table->append({12345, 458.7f});

    opossum::StorageManager::get().add_table("table_a", std::move(_test_table));

    _gt = std::make_shared<opossum::GetTable>("table_a");
  }

  virtual void TearDown() { opossum::StorageManager::get().drop_table("table_a"); }

 public:
  std::shared_ptr<opossum::Table> _test_table;
  std::shared_ptr<opossum::GetTable> _gt;
};

TEST_F(OperatorsTableScanImplTest, SingleScanReturnsCorrectRowCount) {
  std::unique_ptr<AbstractOperatorImpl> scan(
      make_unique_by_column_type<AbstractOperatorImpl, TableScanImpl>("int", _gt, "a", ">=", 1234));
  scan->execute();
  EXPECT_EQ(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[1]), 12345);
  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[0]), 123);
  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[1]), 123);

  EXPECT_EQ(scan->get_output()->row_count(), (uint32_t)2);
}

TEST_F(OperatorsTableScanImplTest, UnknownOperatorThrowsException) {
  std::unique_ptr<AbstractOperatorImpl> scan(
      make_unique_by_column_type<AbstractOperatorImpl, TableScanImpl>("int", _gt, "a", "xor", 10));

  EXPECT_THROW(scan->execute(), std::exception);
}

TEST_F(OperatorsTableScanImplTest, UnsortedPosListInReferenceColumn) {
  std::shared_ptr<opossum::Table> test_ref_table = std::make_shared<opossum::Table>(opossum::Table(2));

  std::shared_ptr<PosList> pos_list = std::make_shared<PosList>();
  pos_list->emplace_back(row_id_from_chunk_id_and_chunk_offset(0, 1));
  pos_list->emplace_back(row_id_from_chunk_id_and_chunk_offset(1, 0));
  pos_list->emplace_back(row_id_from_chunk_id_and_chunk_offset(0, 0));

  for (size_t column_id = 0; column_id < _gt->get_output()->col_count(); ++column_id) {
    auto ref = std::make_shared<ReferenceColumn>(_gt->get_output(), column_id, pos_list);

    test_ref_table->add_column(_gt->get_output()->column_name(column_id), _gt->get_output()->column_type(column_id),
                               false);

    test_ref_table->get_chunk(0).add_column(ref);
  }

  opossum::StorageManager::get().add_table("table_ref", std::move(test_ref_table));

  std::shared_ptr<opossum::GetTable> gt_ref = std::make_shared<opossum::GetTable>("table_ref");
  std::unique_ptr<AbstractOperatorImpl> scan(
      make_unique_by_column_type<AbstractOperatorImpl, TableScanImpl>("int", gt_ref, "a", "!=", 1234));
  scan->execute();

  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[0]), 1234);
  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[1]), 1234);

  EXPECT_EQ(scan->get_output()->row_count(), (uint32_t)2);
  opossum::StorageManager::get().drop_table("table_ref");
}

TEST_F(OperatorsTableScanImplTest, nullptr_pos_list_in_reference_column) {
  std::shared_ptr<opossum::Table> test_ref_table = std::make_shared<opossum::Table>(opossum::Table(2));

  for (size_t column_id = 0; column_id < _gt->get_output()->col_count(); ++column_id) {
    auto ref = std::make_shared<ReferenceColumn>(_gt->get_output(), column_id, nullptr);

    test_ref_table->add_column(_gt->get_output()->column_name(column_id), _gt->get_output()->column_type(column_id),
                               false);

    test_ref_table->get_chunk(0).add_column(ref);
  }

  opossum::StorageManager::get().add_table("table_ref", std::move(test_ref_table));

  std::shared_ptr<opossum::GetTable> gt_ref = std::make_shared<opossum::GetTable>("table_ref");
  std::unique_ptr<AbstractOperatorImpl> scan(
      make_unique_by_column_type<AbstractOperatorImpl, TableScanImpl>("int", gt_ref, "a", "!=", 1234));
  scan->execute();

  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[0]), 1234);
  EXPECT_NE(type_cast<int>((*(scan->get_output()->get_chunk(0).get_column(0)))[1]), 1234);

  EXPECT_EQ(scan->get_output()->row_count(), (uint32_t)2);
}

}  // namespace opossum
