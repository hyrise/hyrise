#include "base_test.hpp"

#include "storage/table.hpp"
#include <synthetic_table_generator.hpp>
#include <operators/get_table.hpp>

#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"


namespace opossum {
    class CalibrationLQPGeneratorTest : public BaseTest {
    protected:
        void SetUp() override {

          auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
                  {DataType::Double, DataType::Float, DataType::Int,
                   DataType::Long, DataType::String, DataType::Null},
                  {EncodingType::Dictionary},
                  {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
                  {100},
                  {150, 3000, 6000, 100, 200, 300, 600, 25, 1500, 200, 800, 5, 100}
          });
          auto table_generator = CalibrationTableGenerator(table_config);
          _tables = table_generator.generate();

          for (const auto& table : _tables){
            Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());
          }
        }

        std::vector<std::shared_ptr<const CalibrationTableWrapper>> _tables;
    };

    TEST_F(CalibrationLQPGeneratorTest, NullSmokeTest){
          auto lqp_generator = CalibrationLQPGenerator();
          const auto table = _tables.at(5);
          lqp_generator.generate(OperatorType::TableScan, table);
          ASSERT_GT(lqp_generator.get_lqps().size(), 0);
    }

    TEST_F(CalibrationLQPGeneratorTest, NumericTableScan){
          auto lqp_generator = CalibrationLQPGenerator();
          const auto table = _tables.at(0);
          lqp_generator.generate(OperatorType::TableScan, table);
          for (const std::shared_ptr<AbstractLQPNode>& lqp : lqp_generator.get_lqps()) {
            const auto pqp = LQPTranslator{}.translate_node(lqp);
            ASSERT_TRUE(pqp->type() == OperatorType::TableScan);
          }

          ASSERT_GT(lqp_generator.get_lqps().size(), 0);
    }

    TEST_F(CalibrationLQPGeneratorTest, ReferenceScans){
          auto lqp_generator = CalibrationLQPGenerator();
          const auto table = _tables.at(0);
          lqp_generator.generate(OperatorType::TableScan, table);

          bool found_reference_scan = false;
          for (const std::shared_ptr<AbstractLQPNode>& lqp : lqp_generator.get_lqps()) {
            const auto pqp = LQPTranslator{}.translate_node(lqp);

            if (pqp->type() == OperatorType::TableScan &&
              !dynamic_cast<const opossum::GetTable*>(pqp->input_left().get())) {
              found_reference_scan = true;
            }
          }

          ASSERT_TRUE(found_reference_scan);
    }

}  // namespace opossum
