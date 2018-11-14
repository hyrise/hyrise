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

#include "query/calibration_query_generator_projection.hpp"

namespace opossum {

    class CalibrationQueryGeneratorProjectionTest : public BaseTest {
    protected:
        void SetUp() override {}
    };

    TEST_F(CalibrationQueryGeneratorProjectionTest, CheckColumns) {
        const auto columns = {
                LQPColumnReference(MockNode::make(), ColumnID{0}),
                LQPColumnReference(MockNode::make(), ColumnID{1}),
                LQPColumnReference(MockNode::make(), ColumnID{2})
        };
        auto projection = CalibrationQueryGeneratorProjection::generate_projection(columns);

        ASSERT_TRUE(projection);
        EXPECT_LT(projection->column_expressions().size(), columns.size());
    }

}  // namespace opossum
