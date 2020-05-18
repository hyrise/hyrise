#include <regex>
#include <utility>

#include "base_test.hpp"

#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan/column_vs_column_table_scan_impl.hpp"
#include "storage/table.hpp"
#include "synthetic_table_generator.hpp"

namespace opossum {
class CalibrationLQPGeneratorTest : public BaseTest {
 protected:
  void TearDown() override {
    // Drop all tables that have been generated
    auto table_names = Hyrise::get().storage_manager.table_names();
    for (const auto& table_name : table_names) {
      Hyrise::get().storage_manager.drop_table(table_name);
    }
  }

  static std::vector<std::shared_ptr<const CalibrationTableWrapper>> _generate_data(
      std::shared_ptr<TableGeneratorConfig> table_config) {
    auto table_generator = CalibrationTableGenerator(std::move(table_config));
    auto tables = table_generator.generate();

    for (const auto& table : tables) {
      Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());
    }

    return tables;
  }
};

TEST_F(CalibrationLQPGeneratorTest, NullColumn) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(
      TableGeneratorConfig{{DataType::Float, DataType::Null},
                           {EncodingType::Dictionary},
                           {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
                           {100},
                           {10}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);
  ASSERT_GT(lqp_generator.get_lqps().size(), 0);
}

TEST_F(CalibrationLQPGeneratorTest, EmptyColumn) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(
      TableGeneratorConfig{{
                               DataType::Int,
                           },
                           {EncodingType::Dictionary},
                           {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
                           {100},
                           {0}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);
  ASSERT_GT(lqp_generator.get_lqps().size(), 0);
}

TEST_F(CalibrationLQPGeneratorTest, NumericTableScan) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(
      TableGeneratorConfig{{DataType::Float},
                           {EncodingType::Dictionary},
                           {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
                           {100},
                           {100}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);
  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqp_generator.get_lqps()) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    ASSERT_TRUE(pqp->type() == OperatorType::TableScan);
  }

  ASSERT_GT(lqp_generator.get_lqps().size(), 0);
}

TEST_F(CalibrationLQPGeneratorTest, ReferenceScans) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(
      TableGeneratorConfig{{DataType::Float},
                           {EncodingType::Dictionary},
                           {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
                           {100},
                           {100}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);

  bool found_reference_scan = false;
  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqp_generator.get_lqps()) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);

    if (pqp->type() == OperatorType::TableScan && !dynamic_cast<const opossum::GetTable*>(pqp->input_left().get())) {
      found_reference_scan = true;
    }
  }

  ASSERT_TRUE(found_reference_scan);
}

TEST_F(CalibrationLQPGeneratorTest, ColumnVsColumnScan) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
      {DataType::Float},
      // uses alternating encoding types to force generation of several columns with the same datatype
      // (generation of ColumnVsColumn Scans is currently only supported on columns with the same type)
      {EncodingType::Dictionary, EncodingType::Unencoded},
      {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
      {10},
      {100}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);

  // we check whether two columns, identified by their names are compared to each other
  std::regex detectColumnVsColumnScan("float_.*_0 > float_.*_0");

  bool found_column_vs_column_scan = false;
  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqp_generator.get_lqps()) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    if (regex_search(pqp->description(), detectColumnVsColumnScan)) {
      found_column_vs_column_scan = true;
    }
  }

  ASSERT_TRUE(found_column_vs_column_scan);
}
}  // namespace opossum
