#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsProjectionTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper->execute();

    _table_wrapper_int = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int.tbl", 2));
    _table_wrapper_int->execute();

    std::shared_ptr<Table> test_table_dict = load_table("src/test/tables/int_int_int.tbl", 2);
    DictionaryCompression::compress_table(*test_table_dict);

    _table_wrapper_int_dict = std::make_shared<TableWrapper>(std::move(test_table_dict));
    _table_wrapper_int_dict->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_int, _table_wrapper_int_dict;
};

TEST_F(OperatorsProjectionTest, SingleColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  Projection::ColumnExpressions column_expressions({ExpressionNode::create_column_reference("", "a")});
  auto projection = std::make_shared<Projection>(_table_wrapper, column_expressions);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}
//
//TEST_F(OperatorsProjectionTest, SingleColumnOldInterface) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);
//
//  std::vector<std::string> columns = {"a"};
//  auto projection = std::make_shared<Projection>(_table_wrapper, columns);
//  projection->execute();
//  auto out = projection->get_output();
//  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, DoubleProject) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 3);
//
//  Projection::ProjectionDefinitions definitions{{"$a", "int", "a"}};
//  auto projection1 = std::make_shared<Projection>(_table_wrapper, definitions);
//  projection1->execute();
//
//  auto projection2 = std::make_shared<Projection>(projection1, definitions);
//  projection2->execute();
//
//  EXPECT_TABLE_EQ(projection2->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, AllColumns) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float_int.tbl", 2);
//
//  Projection::ProjectionDefinitions definitions{{"$b", "float", "b"},
//                                                Projection::ProjectionDefinition{"$a", "int", "a"}};
//  auto projection = std::make_shared<Projection>(_table_wrapper, definitions);
//  projection->execute();
//
//  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, ConstantArithmeticProjection) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_fix_values.tbl", 2);
//
//  Projection::ProjectionDefinitions definitions{{"2+2", "int", "fix"}};
//  auto projection = std::make_shared<Projection>(_table_wrapper_int, definitions);
//  projection->execute();
//
//  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, VariableArithmeticProjection) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);
//
//  Projection::ProjectionDefinitions definitions{{"$a+$b+$c", "int", "sum"}};
//  auto projection = std::make_shared<Projection>(_table_wrapper_int, definitions);
//  projection->execute();
//
//  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, VariableArithmeticWithDictProjection) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);
//
//  Projection::ProjectionDefinitions definitions{{"$a+$b+$c", "int", "sum"}};
//  auto projection = std::make_shared<Projection>(_table_wrapper_int_dict, definitions);
//  projection->execute();
//
//  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, VariableArithmeticWithRefProjection) {
//  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);
//
//  // creates ref_columns
//  auto table_scan = std::make_shared<TableScan>(_table_wrapper_int_dict, "a", ScanType::OpGreaterThan, "0");
//  table_scan->execute();
//
//  Projection::ProjectionDefinitions definitions{{"$a+$b+$c", "int", "sum"}};
//  auto projection = std::make_shared<Projection>(table_scan, definitions);
//  projection->execute();
//
//  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
//}
//
//TEST_F(OperatorsProjectionTest, ValueColumnCount) {
//  Projection::ProjectionDefinitions columns{{"$a", "int", "a"}, Projection::ProjectionDefinition{"$b", "int", "b"}};
//  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, columns);
//  projection_1->execute();
//  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
//  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)3);
//
//  columns = {{"$b", "int", "b"}};
//  auto projection_2 = std::make_shared<opossum::Projection>(_table_wrapper, columns);
//  projection_2->execute();
//  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
//  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)3);
//
//  columns = {{"$a", "int", "a"}};
//  auto projection_3 = std::make_shared<opossum::Projection>(_table_wrapper, columns);
//  projection_3->execute();
//  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
//  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)3);
//}
//
//// TODO(anyone): refactor test
//TEST_F(OperatorsProjectionTest, ReferenceColumnCount) {
//  auto scan = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ScanType::OpEquals, 1234);
//  scan->execute();
//
//  Projection::ProjectionDefinitions columns{{"$a", "int", "a"}, Projection::ProjectionDefinition{"$b", "int", "b"}};
//  auto projection_1 = std::make_shared<opossum::Projection>(scan, columns);
//  projection_1->execute();
//  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
//  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)1);
//
//  columns = {{"$a", "int", "a"}};
//  auto projection_2 = std::make_shared<opossum::Projection>(scan, columns);
//  projection_2->execute();
//  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
//  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)1);
//
//  columns = {{"$b", "int", "b"}};
//  auto projection_3 = std::make_shared<opossum::Projection>(scan, columns);
//  projection_3->execute();
//  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
//  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)1);
//}
//
//TEST_F(OperatorsProjectionTest, NumInputTables) {
//  Projection::ProjectionDefinitions columns = {{"$a", "int", "a"}, Projection::ProjectionDefinition{"$b", "int", "b"}};
//  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, columns);
//
//  EXPECT_EQ(projection_1->num_in_tables(), 1);
//}
//
//TEST_F(OperatorsProjectionTest, NumOutputTables) {
//  Projection::ProjectionDefinitions columns = {{"$a", "int", "a"}, Projection::ProjectionDefinition{"$b", "int", "b"}};
//  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, columns);
//
//  EXPECT_EQ(projection_1->num_out_tables(), 1);
//}
//
//TEST_F(OperatorsProjectionTest, OperatorName) {
//  Projection::ProjectionDefinitions columns = {{"$a", "int", "a"}, Projection::ProjectionDefinition{"$b", "int", "b"}};
//  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, columns);
//
//  EXPECT_EQ(projection_1->name(), "Projection");
//}

}  // namespace opossum
