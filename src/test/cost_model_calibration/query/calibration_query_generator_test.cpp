#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "configuration/calibration_column_specification.hpp"
#include "configuration/calibration_configuration.hpp"
#include "query/calibration_query_generator.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class CalibrationQueryGeneratorTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& manager = Hyrise::get().storage_manager;
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_string_float_calibration.tbl", 1u));
  }
};
/*
TEST_F(CalibrationQueryGeneratorTest, SimpleTest) {
  //             Query Generator expects one column with the name 'column_pk', which is handled as primary key
  // TODO(Sven): is this still necessary?
  std::vector<CalibrationColumnSpecification> columns = {
      CalibrationColumnSpecification{"column_pk", DataType::Int, "uniform", false, 100, EncodingType::Unencoded, 1},
      CalibrationColumnSpecification{"a", DataType::Int, "uniform", false, 100, EncodingType::Unencoded, 1},
      CalibrationColumnSpecification{"b", DataType::Int, "uniform", false, 100, EncodingType::Unencoded, 1},
      CalibrationColumnSpecification{"c", DataType::Int, "uniform", false, 100, EncodingType::Unencoded, 1},
      CalibrationColumnSpecification{"d", DataType::String, "uniform", false, 100, EncodingType::Unencoded, 1},
  };

  const CalibrationConfiguration configuration{
      {}, "", "", 1, {EncodingType::Unencoded}, {DataType::Int, DataType::String}, {0.1f, 0.8f}, {}, false};

  const CalibrationQueryGenerator generator({{"SomeTable", 100}}, columns, configuration);
  const auto query_templates = generator.generate_queries();

  for (const auto& query : query_templates) {
    query->print();
  }
}
 */

}  // namespace opossum
