#include "base_test.hpp"

#include "storage/table.hpp"
#include "../../calibration/calibration_table_generator.hpp"
//#include <synthetic_table_generator.hpp>

#include "../../calibration/lqp_generator.hpp"
#include "../../calibration/lqp_generator.cpp"


namespace opossum {
    class LQPGeneratorTest : public BaseTest {
    protected:
        void SetUp() override {

          auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
                  {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
                  {EncodingType::Dictionary},
                  {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
                  {100},
                  {1500, 3000, 6000, 10000, 20000, 30000, 60175, 25, 15000, 2000, 8000, 5, 100}
          });
          auto table_generator = CalibrationTableGenerator(table_config);
          _tables = table_generator.generate();
        }

        std::vector<std::shared_ptr<const CalibrationTableWrapper>> _tables;
    };
    TEST_F(LQPGeneratorTest, NullSmokeTest){
          auto lqp_generator = LQPGenerator();
          lqp_generator.generate(OperatorType::TableScan, _tables.at(5));
          ASSERT_EQ(lqp_generator.get_lqps().size(), 0);

    }
}  // namespace opossum
