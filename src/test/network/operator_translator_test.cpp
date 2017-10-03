#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "network/operator_translator.hpp"
#include "network/opossum.pb.wrapper.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/difference.hpp"
#include "operators/export_binary.hpp"
#include "operators/export_csv.hpp"
#include "operators/get_table.hpp"
#include "operators/import_csv.hpp"
#include "operators/index_column_scan.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/print.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = load_table("src/test/tables/int_string2.tbl", 2);
    // At this point it would be too complicated to replace the test tables with TableWrapper. Because the TableWrapper
    // operator would have to be translated by the network module.
    StorageManager::get().add_table("TestTable", _test_table);

    std::shared_ptr<Table> test_table_int_float = load_table("src/test/tables/int_float.tbl", 5);
    StorageManager::get().add_table("table_int_float", std::move(test_table_int_float));

    std::shared_ptr<Table> test_table_int_float_2 = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_int_float_2", std::move(test_table_int_float_2));

    std::shared_ptr<Table> test_table_int_float_3 = load_table("src/test/tables/int_float3.tbl", 2);
    StorageManager::get().add_table("table_int_float_3", std::move(test_table_int_float_3));

    std::shared_ptr<Table> test_table_a = load_table("src/test/tables/int.tbl", 5);
    StorageManager::get().add_table("table_a", std::move(test_table_a));

    std::shared_ptr<Table> test_table_b = load_table("src/test/tables/float.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(test_table_b));
  }

  std::shared_ptr<Table> _test_table;
};

TEST_F(OperatorTranslatorTest, GetTable) {
  auto msg = proto::OperatorVariant();
  proto::GetTableOperator* get_table_operator = msg.mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 1ul);

  auto task = tasks.at(0);
  ASSERT_EQ(root_task, task);
  const auto get_table = std::dynamic_pointer_cast<GetTable>(task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  EXPECT_EQ(get_table->get_output(), _test_table);
}

TEST_F(OperatorTranslatorTest, ScanTableInt) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::TableScanOperator* table_scan_operator = msg.mutable_table_scan();
  table_scan_operator->set_column_id(ColumnID{0});
  table_scan_operator->set_filter_operator(proto::ScanType::OpEquals);
  proto::Variant* variant = table_scan_operator->mutable_value();
  variant->set_value_int(123);
  proto::GetTableOperator* get_table_operator = table_scan_operator->mutable_input_operator()->mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto table_scan_task = tasks.at(1);
  auto table_scan = std::dynamic_pointer_cast<TableScan>(table_scan_task->get_operator());
  ASSERT_TRUE(table_scan_task);
  ASSERT_EQ(root_task, table_scan_task);
  table_scan->execute();

  EXPECT_TABLE_EQ(table_scan->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, ScanTableIntBetween) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::TableScanOperator* table_scan_operator = msg.mutable_table_scan();
  table_scan_operator->set_column_id(ColumnID{0});
  table_scan_operator->set_filter_operator(proto::ScanType::OpBetween);
  proto::Variant* variant = table_scan_operator->mutable_value();
  variant->set_value_int(122);
  proto::Variant* variant2 = table_scan_operator->mutable_value2();
  variant2->set_value_int(124);
  proto::GetTableOperator* get_table_operator = table_scan_operator->mutable_input_operator()->mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto table_scan_task = tasks.at(1);
  auto table_scan = std::dynamic_pointer_cast<TableScan>(table_scan_task->get_operator());
  ASSERT_TRUE(table_scan_task);
  ASSERT_EQ(root_task, table_scan_task);
  table_scan->execute();

  EXPECT_TABLE_EQ(table_scan->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, ScanTableString) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::TableScanOperator* table_scan_operator = msg.mutable_table_scan();
  table_scan_operator->set_column_id(ColumnID{1});
  table_scan_operator->set_filter_operator(proto::ScanType::OpEquals);
  proto::Variant* variant = table_scan_operator->mutable_value();
  variant->set_value_string("A");
  proto::GetTableOperator* get_table_operator = table_scan_operator->mutable_input_operator()->mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto table_scan_task = tasks.at(1);
  auto table_scan = std::dynamic_pointer_cast<TableScan>(table_scan_task->get_operator());
  ASSERT_TRUE(table_scan_task);
  ASSERT_EQ(root_task, table_scan_task);
  table_scan->execute();

  EXPECT_TABLE_EQ(table_scan->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, Projection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  auto msg = proto::OperatorVariant();
  proto::ProjectionOperator* projection_operator = msg.mutable_projection();
  projection_operator->add_column_id(0);
  proto::GetTableOperator* get_table_operator = projection_operator->mutable_input_operator()->mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto projection_task = tasks.at(1);
  auto projection = std::dynamic_pointer_cast<Projection>(projection_task->get_operator());
  ASSERT_TRUE(projection);
  ASSERT_EQ(root_task, projection_task);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, Product) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_product.tbl", 3);

  auto msg = proto::OperatorVariant();
  proto::ProductOperator* product_operation = msg.mutable_product();
  product_operation->mutable_left_operator()->mutable_get_table()->set_table_name("table_a");
  product_operation->mutable_right_operator()->mutable_get_table()->set_table_name("table_b");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 3ul);

  auto get_table_task_a = tasks.at(0);
  auto get_table_a = std::dynamic_pointer_cast<GetTable>(get_table_task_a->get_operator());
  ASSERT_TRUE(get_table_a);
  get_table_a->execute();

  auto get_table_task_b = tasks.at(1);
  auto get_table_b = std::dynamic_pointer_cast<GetTable>(get_table_task_b->get_operator());
  ASSERT_TRUE(get_table_b);
  get_table_b->execute();

  auto product_task = tasks.at(2);
  auto product = std::dynamic_pointer_cast<Product>(product_task->get_operator());
  ASSERT_TRUE(product);
  ASSERT_EQ(root_task, product_task);
  product->execute();

  EXPECT_TABLE_EQ(product->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, AscendingSort) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::SortOperator* sort_operation = msg.mutable_sort();
  sort_operation->mutable_input_operator()->mutable_get_table()->set_table_name("table_int_float");
  sort_operation->set_column_id(ColumnID{0});
  sort_operation->set_order_by_mode(opossum::proto::SortOperator::Ascending);

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  auto get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto sort_task = tasks.at(1);
  auto sort = std::dynamic_pointer_cast<Sort>(sort_task->get_operator());
  ASSERT_TRUE(sort);
  ASSERT_EQ(root_task, sort_task);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorTranslatorTest, DescendingSort) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::SortOperator* sort_operation = msg.mutable_sort();
  sort_operation->mutable_input_operator()->mutable_get_table()->set_table_name("table_int_float");
  sort_operation->set_column_id(ColumnID{0});
  sort_operation->set_order_by_mode(opossum::proto::SortOperator::Descending);

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  auto get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto sort_task = tasks.at(1);
  auto sort = std::dynamic_pointer_cast<Sort>(sort_task->get_operator());
  ASSERT_TRUE(sort);
  ASSERT_EQ(root_task, sort_task);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorTranslatorTest, UnionOfTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_union.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::UnionAllOperator* union_all_operation = msg.mutable_union_all();
  union_all_operation->mutable_input_operator1()->mutable_get_table()->set_table_name("table_int_float");
  union_all_operation->mutable_input_operator2()->mutable_get_table()->set_table_name("table_int_float_2");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 3ul);

  auto get_table_task_1 = tasks.at(0);
  auto get_table_1 = std::dynamic_pointer_cast<GetTable>(get_table_task_1->get_operator());
  ASSERT_TRUE(get_table_1);
  get_table_1->execute();

  auto get_table_task_2 = tasks.at(1);
  auto get_table_2 = std::dynamic_pointer_cast<GetTable>(get_table_task_2->get_operator());
  ASSERT_TRUE(get_table_2);
  get_table_2->execute();

  auto union_all_task = tasks.at(2);
  auto union_all = std::dynamic_pointer_cast<UnionAll>(union_all_task->get_operator());
  ASSERT_TRUE(union_all);
  ASSERT_EQ(root_task, union_all_task);
  union_all->execute();

  EXPECT_TABLE_EQ(union_all->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, TableScanAndProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered_projected.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::TableScanOperator* table_scan_operator = msg.mutable_table_scan();
  table_scan_operator->set_column_id(ColumnID{0});
  table_scan_operator->set_filter_operator(proto::ScanType::OpEquals);
  proto::Variant* variant = table_scan_operator->mutable_value();
  variant->set_value_int(123);
  proto::ProjectionOperator* projection_operator = table_scan_operator->mutable_input_operator()->mutable_projection();
  projection_operator->add_column_id(0);
  proto::GetTableOperator* get_table_operator = projection_operator->mutable_input_operator()->mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 3ul);

  auto get_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto projection_task = tasks.at(1);
  auto projection = std::dynamic_pointer_cast<Projection>(projection_task->get_operator());
  ASSERT_TRUE(projection);
  projection->execute();

  auto table_scan_task = tasks.at(2);
  auto table_scan = std::dynamic_pointer_cast<TableScan>(table_scan_task->get_operator());
  ASSERT_TRUE(table_scan_task);
  ASSERT_EQ(root_task, table_scan_task);
  table_scan->execute();

  EXPECT_TABLE_EQ(table_scan->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, ImportCsv) {
  auto msg = proto::OperatorVariant();
  proto::ImportCsvOperator* import_csv_operator = msg.mutable_import_csv();
  import_csv_operator->set_directory("src/test/csv");
  import_csv_operator->set_filename("float.csv");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 1ul);

  auto task = tasks.at(0);
  ASSERT_EQ(root_task, task);
  const auto import_csv = std::dynamic_pointer_cast<ImportCsv>(task->get_operator());
  ASSERT_TRUE(import_csv);
  import_csv->execute();

  auto expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ(import_csv->get_output(), expected_table);
}

TEST_F(OperatorTranslatorTest, ExportCsv) {
  auto msg = proto::OperatorVariant();
  proto::ExportCsvOperator* export_operator = msg.mutable_export_csv();
  export_operator->set_directory("src/test/csv");
  export_operator->set_filename("tmp_float");
  export_operator->mutable_input_operator()->mutable_get_table()->set_table_name("table_int_float");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  auto get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);

  auto task = tasks.at(1);
  ASSERT_EQ(root_task, task);
  const auto export_csv = std::dynamic_pointer_cast<ExportCsv>(task->get_operator());
  ASSERT_TRUE(export_csv);
}

TEST_F(OperatorTranslatorTest, ExportBinary) {
  auto msg = proto::OperatorVariant();
  proto::ExportBinaryOperator* export_operator = msg.mutable_export_binary();
  export_operator->set_filename("tmp_float");
  export_operator->mutable_input_operator()->mutable_get_table()->set_table_name("table_int_float");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  auto get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);

  auto task = tasks.at(1);
  ASSERT_EQ(root_task, task);
  const auto export_csv = std::dynamic_pointer_cast<ExportBinary>(task->get_operator());
  ASSERT_TRUE(export_csv);
}

TEST_F(OperatorTranslatorTest, Print) {
  auto msg = proto::OperatorVariant();
  auto print_operator = msg.mutable_print();
  print_operator->mutable_input_operator()->mutable_get_table()->set_table_name("table_int_float");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  auto get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto task = tasks.at(1);
  ASSERT_EQ(root_task, task);
  const auto print = std::dynamic_pointer_cast<Print>(task->get_operator());
  ASSERT_TRUE(print);
  // suppress printing
  std::ostringstream local;
  auto cout_buf = std::cout.rdbuf();
  std::cout.rdbuf(local.rdbuf());
  print->execute();
  std::cout.rdbuf(cout_buf);

  auto expected_table = load_table("src/test/tables/int_float.tbl", 5);
  EXPECT_TABLE_EQ(print->get_output(), expected_table);
}

TEST_F(OperatorTranslatorTest, Difference) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 2);

  auto msg = proto::OperatorVariant();
  auto difference_operator = msg.mutable_difference();
  difference_operator->mutable_left_operator()->mutable_get_table()->set_table_name("table_int_float");
  difference_operator->mutable_right_operator()->mutable_get_table()->set_table_name("table_int_float_3");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 3ul);

  auto get_left_table_task = tasks.at(0);
  auto get_left_table = std::dynamic_pointer_cast<GetTable>(get_left_table_task->get_operator());
  ASSERT_TRUE(get_left_table);
  get_left_table->execute();

  auto get_right_table_task = tasks.at(1);
  auto get_right_table = std::dynamic_pointer_cast<GetTable>(get_right_table_task->get_operator());
  ASSERT_TRUE(get_right_table);
  get_right_table->execute();

  auto difference_task = tasks.at(2);
  auto difference = std::dynamic_pointer_cast<Difference>(difference_task->get_operator());
  ASSERT_TRUE(difference_task);
  ASSERT_EQ(root_task, difference_task);
  difference->execute();

  EXPECT_TABLE_EQ(difference->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, IndexColumnScanInt) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered.tbl", 2);

  auto msg = proto::OperatorVariant();
  proto::IndexColumnScanOperator* index_column_scan_operator = msg.mutable_index_column_scan();
  index_column_scan_operator->set_column_id(ColumnID{0});
  index_column_scan_operator->set_filter_operator(proto::ScanType::OpEquals);
  proto::Variant* variant = index_column_scan_operator->mutable_value();
  variant->set_value_int(123);
  proto::GetTableOperator* get_table_operator =
      index_column_scan_operator->mutable_input_operator()->mutable_get_table();
  get_table_operator->set_table_name("TestTable");

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 2ul);

  auto get_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_table = std::dynamic_pointer_cast<GetTable>(get_table_task->get_operator());
  ASSERT_TRUE(get_table);
  get_table->execute();

  auto index_column_scan_task = tasks.at(1);
  auto index_column_scan = std::dynamic_pointer_cast<IndexColumnScan>(index_column_scan_task->get_operator());
  ASSERT_TRUE(index_column_scan_task);
  ASSERT_EQ(root_task, index_column_scan_task);
  index_column_scan->execute();

  EXPECT_TABLE_EQ(index_column_scan->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, DISABLED_NestedLoopJoinModes) {
  auto msg = proto::OperatorVariant();
  proto::NestedLoopJoinOperator* join_operation = msg.mutable_nested_loop_join();
  join_operation->mutable_left_operator()->mutable_get_table()->set_table_name("table_int_float");
  join_operation->mutable_right_operator()->mutable_get_table()->set_table_name("table_int_float_2");
  join_operation->set_op(proto::ScanType::OpEquals);

  auto proto_column_id_left = proto::OptionalColumnID();
  proto_column_id_left.set_value(0);

  auto proto_column_id_right = proto::OptionalColumnID();
  proto_column_id_left.set_value(0);

  join_operation->set_allocated_left_column_id(&proto_column_id_left);
  join_operation->set_allocated_right_column_id(&proto_column_id_right);

  auto modes = {proto::NestedLoopJoinOperator::Inner, proto::NestedLoopJoinOperator::Left,
                proto::NestedLoopJoinOperator::Right, proto::NestedLoopJoinOperator::Outer,
                proto::NestedLoopJoinOperator::Cross, proto::NestedLoopJoinOperator::Natural,
                proto::NestedLoopJoinOperator::Self};

  for (auto mode : modes) {
    join_operation->set_mode(mode);

    OperatorTranslator translator;
    auto& tasks = translator.build_tasks_from_proto(msg);
    ASSERT_EQ(tasks.size(), 3ul);
    auto join_task = tasks.at(2);
    auto join = std::dynamic_pointer_cast<JoinNestedLoopA>(join_task->get_operator());
    ASSERT_TRUE(join_task);
  }
}

TEST_F(OperatorTranslatorTest, DISABLED_NestedLoopJoinWithColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_left_join.tbl", 1);

  auto msg = proto::OperatorVariant();
  proto::NestedLoopJoinOperator* join_operation = msg.mutable_nested_loop_join();
  join_operation->mutable_left_operator()->mutable_get_table()->set_table_name("table_int_float");
  join_operation->mutable_right_operator()->mutable_get_table()->set_table_name("table_int_float_2");
  join_operation->set_mode(proto::NestedLoopJoinOperator::Left);
  join_operation->set_op(proto::ScanType::OpEquals);

  auto proto_column_id_left = proto::OptionalColumnID();
  proto_column_id_left.set_value(0);

  auto proto_column_id_right = proto::OptionalColumnID();
  proto_column_id_left.set_value(0);

  join_operation->set_allocated_left_column_id(&proto_column_id_left);
  join_operation->set_allocated_right_column_id(&proto_column_id_right);

  OperatorTranslator translator;
  auto& tasks = translator.build_tasks_from_proto(msg);
  auto root_task = translator.root_task();
  ASSERT_EQ(tasks.size(), 3ul);

  auto get_left_table_task = tasks.at(0);
  std::shared_ptr<GetTable> get_left_table = std::dynamic_pointer_cast<GetTable>(get_left_table_task->get_operator());
  ASSERT_TRUE(get_left_table);
  get_left_table->execute();

  auto get_right_table_task = tasks.at(1);
  std::shared_ptr<GetTable> get_right_table = std::dynamic_pointer_cast<GetTable>(get_right_table_task->get_operator());
  ASSERT_TRUE(get_right_table);
  get_right_table->execute();

  auto join_task = tasks.at(2);
  auto join = std::dynamic_pointer_cast<JoinNestedLoopA>(join_task->get_operator());
  ASSERT_TRUE(join_task);
  ASSERT_EQ(root_task, join_task);
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorTranslatorTest, ProjectionMissingInput) {
  auto msg = proto::OperatorVariant();
  proto::ProjectionOperator* projection_operator = msg.mutable_projection();
  projection_operator->add_column_id(0);

  OperatorTranslator translator;
  EXPECT_THROW(translator.build_tasks_from_proto(msg), std::logic_error);
}

TEST_F(OperatorTranslatorTest, ProjectionIncompleteInput) {
  auto msg = proto::OperatorVariant();
  proto::ProjectionOperator* projection_operator = msg.mutable_projection();
  projection_operator->add_column_id(0);
  projection_operator->mutable_input_operator();

  OperatorTranslator translator;
  EXPECT_THROW(translator.build_tasks_from_proto(msg), std::logic_error);
}

}  // namespace opossum
