#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class JitReadWriteTupleTest : public BaseTest {};

TEST_F(JitReadWriteTupleTest, CreateOutputTable) {
  auto write_tuples = std::make_shared<JitWriteTuples>();

  TableColumnDefinitions column_definitions = {{"a", DataType::Int, false},
                                               {"b", DataType::Long, true},
                                               {"c", DataType::Float, false},
                                               {"d", DataType::Double, false},
                                               {"e", DataType::String, true}};

  for (const auto& column_definition : column_definitions) {
    write_tuples->add_output_column_definition(
        column_definition.name, JitTupleValue(column_definition.data_type, column_definition.nullable, 0));
  }

  auto output_table = write_tuples->create_output_table(Table(TableColumnDefinitions{}, TableType::Data, 1));
  ASSERT_EQ(output_table->column_definitions(), column_definitions);
}

TEST_F(JitReadWriteTupleTest, TupleIndicesAreIncremented) {
  auto read_tuples = std::make_shared<JitReadTuples>();

  // Add different kinds of values (input columns, literals, temporary values) to the runtime tuple
  auto tuple_index_1 = read_tuples->add_input_column(DataType::Int, false, ColumnID{0}).tuple_index();
  auto tuple_index_2 = read_tuples->add_literal_value(1).tuple_index();
  auto tuple_index_3 = read_tuples->add_temporary_value();
  auto tuple_index_4 = read_tuples->add_input_column(DataType::Int, false, ColumnID{1}).tuple_index();
  auto tuple_index_5 = read_tuples->add_literal_value("some string").tuple_index();
  auto tuple_index_6 = read_tuples->add_temporary_value();

  // All values should have their own position in the tuple with tuple indices increasing
  ASSERT_LT(tuple_index_1, tuple_index_2);
  ASSERT_LT(tuple_index_2, tuple_index_3);
  ASSERT_LT(tuple_index_3, tuple_index_4);
  ASSERT_LT(tuple_index_4, tuple_index_5);
  ASSERT_LT(tuple_index_5, tuple_index_6);

  // Adding the same input column twice should not create a new value in the tuple
  auto tuple_index_1_b = read_tuples->add_input_column(DataType::Int, false, ColumnID{0}).tuple_index();
  ASSERT_EQ(tuple_index_1, tuple_index_1_b);
}

TEST_F(JitReadWriteTupleTest, LiteralValuesAreInitialized) {
  auto read_tuples = std::make_shared<JitReadTuples>();

  auto int_value = read_tuples->add_literal_value(1);
  auto float_value = read_tuples->add_literal_value(1.23f);
  auto double_value = read_tuples->add_literal_value(12.3);
  auto string_value = read_tuples->add_literal_value("some string");

  // Since we only test literal values here an empty input table is sufficient
  JitRuntimeContext context;
  Table input_table(TableColumnDefinitions{}, TableType::Data);
  read_tuples->before_query(input_table, std::vector<AllTypeVariant>(), context);

  ASSERT_EQ(int_value.get<int32_t>(context), 1);
  ASSERT_EQ(float_value.get<float>(context), 1.23f);
  ASSERT_EQ(double_value.get<double>(context), 12.3);
  ASSERT_EQ(string_value.get<std::string>(context), "some string");
}

TEST_F(JitReadWriteTupleTest, CopyTable) {
  JitRuntimeContext context;

  // Create operator chain that passes from the input tuple to an output table unmodified
  auto read_tuples = std::make_shared<JitReadTuples>();
  auto write_tuples = std::make_shared<JitWriteTuples>();
  read_tuples->set_next_operator(write_tuples);

  // Add all input table columns to pipeline
  auto a_value = read_tuples->add_input_column(DataType::Int, true, ColumnID{0});
  auto b_value = read_tuples->add_input_column(DataType::Float, true, ColumnID{1});
  write_tuples->add_output_column_definition("a", a_value);
  write_tuples->add_output_column_definition("b", b_value);

  // Initialize operators with actual input table
  auto input_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);
  auto output_table = write_tuples->create_output_table(Table(TableColumnDefinitions{}, TableType::Data, 2));
  read_tuples->before_query(*input_table, std::vector<AllTypeVariant>(), context);
  write_tuples->before_query(*output_table, context);

  // Pass each chunk through the pipeline
  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    read_tuples->before_chunk(*input_table, chunk_id, context);
    read_tuples->execute(context);
    write_tuples->after_chunk(input_table, *output_table, context);
  }
  write_tuples->after_query(*output_table, context);

  // Both tables should be equal now
  ASSERT_TRUE(check_table_equal(input_table, output_table, OrderSensitivity::Yes, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));
}

TEST_F(JitReadWriteTupleTest, LimitRowCountIsEvaluated) {
  // Create row count expression
  const int64_t limit_row_count{123};
  const auto row_count_expression = std::make_shared<ValueExpression>(limit_row_count);

  // Initialize operator with row count expression
  auto read_tuples = std::make_shared<JitReadTuples>(false, row_count_expression);

  JitRuntimeContext context;
  // Since we only test literal values here an empty input table is sufficient
  Table input_table(TableColumnDefinitions{}, TableType::Data);
  read_tuples->before_query(input_table, std::vector<AllTypeVariant>(), context);

  ASSERT_EQ(context.limit_rows, limit_row_count);
}

TEST_F(JitReadWriteTupleTest, SetParameterValuesInContext) {
  // Prepare JitReadTuples
  JitReadTuples read_tuples;
  auto tuple_1 = read_tuples.add_parameter(DataType::Long, ParameterID{1});
  auto tuple_2 = read_tuples.add_parameter(DataType::Double, ParameterID{2});

  // Prepare parameter values
  int64_t value_1{1l};
  double value_2{2.};
  std::vector<AllTypeVariant> parameter_values{AllTypeVariant{value_1}, AllTypeVariant{value_2}};

  JitRuntimeContext context;

  // Since we only test parameter values here an empty input table is sufficient
  Table input_table(TableColumnDefinitions{}, TableType::Data);
  read_tuples.before_query(input_table, parameter_values, context);

  ASSERT_EQ(tuple_1.get<int64_t>(context), value_1);
  ASSERT_EQ(tuple_2.get<double>(context), value_2);
}

}  // namespace opossum
