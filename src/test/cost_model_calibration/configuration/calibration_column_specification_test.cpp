#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <json.hpp>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "configuration/calibration_column_specification.hpp"

namespace opossum {

class CalibrationColumnSpecificationTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(CalibrationColumnSpecificationTest, ParseJson) {
  const auto input = nlohmann::json::parse(
      R"({"type": "int", "encoding": "Dictionary","distinct_values": 10000, "column_name": "someColumn"})");
  opossum::CalibrationColumnSpecification column_specification(input);

  CalibrationColumnSpecification expected{"someColumn", DataType::Int, "uniform",
                                          false,        10000,         EncodingType::Dictionary};
  EXPECT_EQ(expected, column_specification);
}

TEST_F(CalibrationColumnSpecificationTest, ParseJsonWithInvalidDataType) {
  const auto input = nlohmann::json::parse(
      R"({"type": "unknownType", "encoding": "Dictionary","distinct_values": 10000, "column_name": "someColumn"})");
  EXPECT_THROW(opossum::CalibrationColumnSpecification _(input), std::exception);
}

TEST_F(CalibrationColumnSpecificationTest, ParseJsonWithInvalidEncoding) {
  const auto input = nlohmann::json::parse(
      R"({"type": "int", "encoding": "unknownEncoding","distinct_values": 10000, "column_name": "someColumn"})");
  EXPECT_THROW(opossum::CalibrationColumnSpecification _(input), std::exception);
}

TEST_F(CalibrationColumnSpecificationTest, ParseJsonWithMissingColumnName) {
  const auto input = nlohmann::json::parse(R"({"type": "int", "encoding": "Dictionary","distinct_values": 10000})");
  EXPECT_THROW(opossum::CalibrationColumnSpecification _(input), std::exception);
}

}  // namespace opossum
