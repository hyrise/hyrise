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

#include "feature/calibration_table_scan_features.hpp"

namespace opossum {

    class CalibrationTableScanFeaturesTest : public BaseTest {
    protected:
        void SetUp() override {}
    };

    TEST_F(CalibrationTableScanFeaturesTest, SimpleTest) {
        const auto num_columns = CalibrationTableScanFeatures::columns.size();
        const auto num_features = CalibrationTableScanFeatures::serialize(CalibrationTableScanFeatures{}).size();

        EXPECT_EQ(num_columns, num_features);
    }

    TEST_F(CalibrationTableScanFeaturesTest, SerializeNullopt) {
        const auto num_columns = CalibrationTableScanFeatures::columns.size();
        const auto num_features = CalibrationTableScanFeatures::serialize({}).size();

        EXPECT_EQ(num_columns, num_features);
    }

}  // namespace opossum
