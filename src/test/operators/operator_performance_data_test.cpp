#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

TEST(OperatorPerformanceDataTest, ElementsAreSet) {
	const TableColumnDefinitions column_definitions{{"a", DataType::Int, false}};
	auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
	table->append({1});
	table->append({2});
	table->append({3});

	auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto table_scan = std::make_shared<TableScan>(table_wrapper, greater_than_(BaseTest::get_column_expression(table_wrapper, ColumnID{0}), 1));
  table_scan->execute();

	auto& performance_data = table_scan->performance_data();
	EXPECT_GT(performance_data.walltime.count(), 0ul);

	EXPECT_TRUE(performance_data.input_row_count_left);
	EXPECT_EQ(3, *(performance_data.input_row_count_left));

	EXPECT_FALSE(performance_data.input_row_count_right);

	EXPECT_EQ(2, performance_data.output_row_count);
}

}  // namespace opossum
