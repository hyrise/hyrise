#include "base_test.hpp"

#include <operators/get_table.hpp>
#include <synthetic_table_generator.hpp>
#include <utility>
#include "storage/table.hpp"

#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"

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

    static std::vector<std::shared_ptr<const CalibrationTableWrapper>>
    _generate_data(std::shared_ptr<TableGeneratorConfig> table_config) {
      auto table_generator = CalibrationTableGenerator(std::move(table_config));
      auto tables = table_generator.generate();

      for (const auto& table : tables) {
        Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());
      }

      return tables;
    }
};


TEST_F(CalibrationLQPGeneratorTest, NullColumn) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
        { DataType::Float, DataType::Null},
        {EncodingType::Dictionary},
        {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
        {100},
        {10}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);
  ASSERT_GT(lqp_generator.get_lqps().size(), 0);
}

TEST_F(CalibrationLQPGeneratorTest, EmptyColumn) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
        { DataType::Int, },
        {EncodingType::Dictionary},
        {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
        {100},
        {0}}));

  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, tables[0]);
  ASSERT_GT(lqp_generator.get_lqps().size(), 0);
}

TEST_F(CalibrationLQPGeneratorTest, NumericTableScan) {
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
      { DataType::Float},
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
  const auto& tables = _generate_data(std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
    { DataType::Float},
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
}  // namespace opossum
