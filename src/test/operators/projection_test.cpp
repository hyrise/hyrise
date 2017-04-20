#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsProjectionTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<GetTable>("table_a");
    _gt->execute();

    std::shared_ptr<Table> test_table_int = load_table("src/test/tables/int_int_int.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(test_table_int));
    _gt_int = std::make_shared<GetTable>("table_b");
    _gt_int->execute();

    std::shared_ptr<Table> test_table_dict = load_table("src/test/tables/int_int_int.tbl", 2);
    test_table_dict->compress_chunk(0);
    test_table_dict->compress_chunk(1);
    StorageManager::get().add_table("table_c", std::move(test_table_dict));
    _gt_int_dict = std::make_shared<GetTable>("table_c");
    _gt_int_dict->execute();
  }

  std::shared_ptr<GetTable> _gt, _gt_int, _gt_int_dict;
};

TEST_F(OperatorsProjectionTest, SingleColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  Projection::ProjectionDefinitions definitions({Projection::ProjectionDefinition{"$a", "int", "a"}});
  auto projection = std::make_shared<Projection>(_gt, definitions);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, SingleColumnOldInterface) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  std::vector<std::string> columns = {"a"};
  auto projection = std::make_shared<Projection>(_gt, columns);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, DoubleProject) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 3);

  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$a", "int", "a"}};
  auto projection1 = std::make_shared<Projection>(_gt, definitions);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, definitions);
  projection2->execute();

  EXPECT_TABLE_EQ(projection2->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, AllColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float_int.tbl", 2);

  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$b", "float", "b"},
                                                Projection::ProjectionDefinition{"$a", "int", "a"}};
  auto projection = std::make_shared<Projection>(_gt, definitions);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ConstantArithmeticProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_fix_values.tbl", 2);

  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"2+2", "int", "fix"}};
  auto projection = std::make_shared<Projection>(_gt_int, definitions);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, VariableArithmeticProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$a+$b+$c", "int", "sum"}};
  auto projection = std::make_shared<Projection>(_gt_int, definitions);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, VariableArithmeticWithDictProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$a+$b+$c", "int", "sum"}};
  auto projection = std::make_shared<Projection>(_gt_int_dict, definitions);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, VariableArithmeticWithRefProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  // creates ref_columns
  auto table_scan = std::make_shared<TableScan>(_gt_int_dict, "a", ">", "0");
  table_scan->execute();

  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$a+$b+$c", "int", "sum"}};
  auto projection = std::make_shared<Projection>(table_scan, definitions);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ValueColumnCount) {
  Projection::ProjectionDefinitions columns{Projection::ProjectionDefinition{"$a", "int", "a"},
                                            Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)3);

  columns = {Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_2 = std::make_shared<opossum::Projection>(_gt, columns);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)3);

  columns = {Projection::ProjectionDefinition{"$a", "int", "a"}};
  auto projection_3 = std::make_shared<opossum::Projection>(_gt, columns);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)3);
}

// TODO(anyone): refactor test
TEST_F(OperatorsProjectionTest, ReferenceColumnCount) {
  auto scan = std::make_shared<opossum::TableScan>(_gt, "a", "=", 1234);
  scan->execute();

  Projection::ProjectionDefinitions columns{Projection::ProjectionDefinition{"$a", "int", "a"},
                                            Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_1 = std::make_shared<opossum::Projection>(scan, columns);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)1);

  columns = {Projection::ProjectionDefinition{"$a", "int", "a"}};
  auto projection_2 = std::make_shared<opossum::Projection>(scan, columns);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)1);

  columns = {Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_3 = std::make_shared<opossum::Projection>(scan, columns);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)1);
}

TEST_F(OperatorsProjectionTest, NumInputTables) {
  Projection::ProjectionDefinitions columns = {Projection::ProjectionDefinition{"$a", "int", "a"},
                                               Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);

  EXPECT_EQ(projection_1->num_in_tables(), 1);
}

TEST_F(OperatorsProjectionTest, NumOutputTables) {
  Projection::ProjectionDefinitions columns = {Projection::ProjectionDefinition{"$a", "int", "a"},
                                               Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);

  EXPECT_EQ(projection_1->num_out_tables(), 1);
}

TEST_F(OperatorsProjectionTest, OperatorName) {
  Projection::ProjectionDefinitions columns = {Projection::ProjectionDefinition{"$a", "int", "a"},
                                               Projection::ProjectionDefinition{"$b", "int", "b"}};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);

  EXPECT_EQ(projection_1->name(), "Projection");
}

}  // namespace opossum
