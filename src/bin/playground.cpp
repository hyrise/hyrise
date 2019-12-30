#include <iostream>
#include <logical_query_plan/mock_node.hpp>
#include <synthetic_table_generator.hpp>
#include <expression/expression_functional.hpp>
#include <logical_query_plan/lqp_translator.hpp>
#include <fstream>
#include <cost_calibration/table_generator.h>
#include <cost_calibration/lqp_generator/table_scan.hpp>

#include "hyrise.hpp"
#include "scheduler/operator_task.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/static_table_node.hpp"

#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

#include "expression/expression_functional.hpp"

#include "types.hpp"

using namespace opossum;  // NOLINT


int main() {

    auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
        {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
        {EncodingType::Dictionary, EncodingType::FixedStringDictionary, EncodingType ::FrameOfReference, EncodingType::LZ4, EncodingType::RunLength, EncodingType::Unencoded},
        {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
        {100000},
        {100, 1000, 10000, 100000, 1000000}
    });

    auto table_generator = TableGenerator(table_config);
    auto tables = table_generator.generate();

    auto lqp_generator = TableScanLQPGenerator();
}