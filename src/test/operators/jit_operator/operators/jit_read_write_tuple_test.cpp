#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
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
        column_definition.name, JitTupleEntry(column_definition.data_type, !column_definition.nullable, 0));
  }

  auto output_table = write_tuples->create_output_table(Table(TableColumnDefinitions{}, TableType::Data, 1));
  ASSERT_EQ(output_table->column_definitions(), column_definitions);
}

TEST_F(JitReadWriteTupleTest, TupleIndicesAreIncremented) {
  auto read_tuples = std::make_shared<JitReadTuples>();

  // Add different kinds of values (input columns, literals, temporary values) to the runtime tuple
  auto tuple_index_1 = read_tuples->add_input_column(DataType::Int, false, ColumnID{0}).tuple_index;
  auto tuple_index_2 = read_tuples->add_literal_value(1).tuple_index;
  auto tuple_index_3 = read_tuples->add_temporary_value();
  auto tuple_index_4 = read_tuples->add_input_column(DataType::Int, false, ColumnID{1}).tuple_index;
  auto tuple_index_5 = read_tuples->add_literal_value("some string").tuple_index;
  auto tuple_index_6 = read_tuples->add_temporary_value();

  // All values should have their own position in the tuple with tuple indices increasing
  ASSERT_LT(tuple_index_1, tuple_index_2);
  ASSERT_LT(tuple_index_2, tuple_index_3);
  ASSERT_LT(tuple_index_3, tuple_index_4);
  ASSERT_LT(tuple_index_4, tuple_index_5);
  ASSERT_LT(tuple_index_5, tuple_index_6);

  // Adding the same input column twice should not create a new value in the tuple
  auto tuple_index_1_b = read_tuples->add_input_column(DataType::Int, false, ColumnID{0}).tuple_index;
  ASSERT_EQ(tuple_index_1, tuple_index_1_b);
}

TEST_F(JitReadWriteTupleTest, LiteralValuesAreInitialized) {
  auto read_tuples = std::make_shared<JitReadTuples>();

  auto int_tuple_entry = read_tuples->add_literal_value(1);
  auto float_tuple_entry = read_tuples->add_literal_value(1.23f);
  auto double_tuple_entry = read_tuples->add_literal_value(12.3);
  auto string_tuple_entry = read_tuples->add_literal_value("some string");

  // Since we only test literal values here an empty input table is sufficient
  JitRuntimeContext context;
  Table input_table(TableColumnDefinitions{}, TableType::Data);
  read_tuples->before_query(input_table, std::vector<AllTypeVariant>(), context);

  ASSERT_EQ(int_tuple_entry.get<int32_t>(context), 1);
  ASSERT_EQ(float_tuple_entry.get<float>(context), 1.23f);
  ASSERT_EQ(double_tuple_entry.get<double>(context), 12.3);
  ASSERT_EQ(string_tuple_entry.get<pmr_string>(context), "some string");
}

TEST_F(JitReadWriteTupleTest, CopyTable) {
  JitRuntimeContext context;

  // Create operator chain that passes from the input tuple to an output table unmodified
  auto read_tuples = std::make_shared<JitReadTuples>();
  auto write_tuples = std::make_shared<JitWriteTuples>();
  read_tuples->set_next_operator(write_tuples);

  // Add all input table columns to pipeline
  auto a_tuple_entry = read_tuples->add_input_column(DataType::Int, false, ColumnID{0});
  auto b_tuple_entry = read_tuples->add_input_column(DataType::Float, false, ColumnID{1});
  write_tuples->add_output_column_definition("a", a_tuple_entry);
  write_tuples->add_output_column_definition("b", b_tuple_entry);

  // Initialize operators with actual input table
  auto input_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);
  auto output_table = write_tuples->create_output_table(Table(TableColumnDefinitions{}, TableType::Data, 2));
  read_tuples->before_query(*input_table, std::vector<AllTypeVariant>(), context);
  write_tuples->before_query(*output_table, context);

  // Pass each chunk through the pipeline
  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    read_tuples->before_chunk(*input_table, chunk_id, std::vector<AllTypeVariant>(), context);
    read_tuples->execute(context);
    write_tuples->after_chunk(input_table, *output_table, context);
  }
  write_tuples->after_query(*output_table, context);

  // Both tables should be equal now
  EXPECT_TABLE_EQ_ORDERED(input_table, output_table);
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

TEST_F(JitReadWriteTupleTest, AddValueIDExpression) {
  JitReadTuples read_tuples;

  auto column_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0});
  auto literal_tuple_entry = read_tuples.add_literal_value(AllTypeVariant{1});
  auto column_expression = std::make_shared<JitExpression>(column_tuple_entry);
  auto literal_expression = std::make_shared<JitExpression>(literal_tuple_entry, AllTypeVariant{1});

  // Value id expressions are only valid if the left input is an input column and the right input is a literal or
  // parameter value
  auto equals = JitExpressionType::Equals;
  auto valid_compare_expression = std::make_shared<JitExpression>(column_expression, equals, literal_expression, 0);
  ASSERT_NO_THROW(read_tuples.add_value_id_expression(valid_compare_expression));

  auto two_input_columns = std::make_shared<JitExpression>(column_expression, equals, column_expression, 0);
  ASSERT_THROW(read_tuples.add_value_id_expression(two_input_columns), std::logic_error);

  auto input_column_on_right = std::make_shared<JitExpression>(literal_expression, equals, column_expression, 0);
  ASSERT_THROW(read_tuples.add_value_id_expression(input_column_on_right), std::logic_error);
}

TEST_F(JitReadWriteTupleTest, BeforeSpecialization) {
  // Check that the function before_specialization() removes any value id expressions that do not use a dictionary-
  // compressed input column and that it updates the used expressions accordingly.

  auto input_table = load_table("resources/test_data/tbl/int_float2.tbl");
  ChunkEncoder::encode_all_chunks(input_table, {EncodingType::Unencoded, EncodingType::Dictionary});

  JitReadTuples read_tuples;
  bool use_actual_value{false};
  auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0}, use_actual_value);
  auto b_tuple_entry = read_tuples.add_input_column(DataType::Float, false, ColumnID{1}, use_actual_value);
  AllTypeVariant value{1};
  auto literal_tuple_entry = read_tuples.add_literal_value(value);

  // Create filter expression
  // clang-format off
  auto expressions_a = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                       JitExpressionType::LessThanEquals,
                                                       std::make_shared<JitExpression>(literal_tuple_entry, value),
                                                       read_tuples.add_temporary_value());
  auto expressions_b = std::make_shared<JitExpression>(std::make_shared<JitExpression>(b_tuple_entry),
                                                       JitExpressionType::LessThanEquals,
                                                       std::make_shared<JitExpression>(literal_tuple_entry, value),
                                                       read_tuples.add_temporary_value());
  // clang-format on

  read_tuples.add_value_id_expression(expressions_a);
  read_tuples.add_value_id_expression(expressions_b);

  ASSERT_EQ(read_tuples.value_id_expressions().size(), 2u);

  std::vector<bool> tuple_non_nullable_information;
  read_tuples.before_specialization(*input_table, tuple_non_nullable_information);

  // Expression a is removed as its used input column is not encoded
  ASSERT_EQ(read_tuples.value_id_expressions().size(), 1u);
  // Only expression b is kept in the list of value id expressions
  ASSERT_EQ(read_tuples.value_id_expressions()[0].jit_expression, expressions_b);

  // Expression a was not modified -> use actual values in comparison
  ASSERT_FALSE(expressions_a->use_value_ids);

  // Expression b was modified -> use value ids in comparison
  ASSERT_TRUE(expressions_b->use_value_ids);

  const auto& input_columns = read_tuples.input_columns();
  // Unencoded column a loads the actual value
  ASSERT_TRUE(input_columns[0].use_actual_value);
  // Dictionary-encoded column b does not load the actual value as it loads the value id
  ASSERT_FALSE(input_columns[1].use_actual_value);
}

TEST_F(JitReadWriteTupleTest, BeforeChunkUpdatesPossibleValueIDExpressions) {
  // Check that the before_chunk() function correctly updates the possible value id expressions if the specialized
  // function cannot be used

  // Prepare input table
  auto input_table = load_table("resources/test_data/tbl/int_float2.tbl", 1);
  std::map<ChunkID, ChunkEncodingSpec> encodings{
      {ChunkID{0}, {EncodingType::Dictionary, EncodingType::Dictionary}},
      {ChunkID{1}, {EncodingType::Unencoded, EncodingType::Unencoded}},
      {ChunkID{2}, {EncodingType::Dictionary, EncodingType::Unencoded}},
  };
  ChunkEncoder::encode_chunks(input_table, {ChunkID{0}, ChunkID{1}, ChunkID{2}}, encodings);

  // Create JitReadTuples operator and JitExpressions
  JitReadTuples read_tuples;
  auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0});
  auto b_tuple_entry = read_tuples.add_input_column(DataType::Float, false, ColumnID{1});
  AllTypeVariant value{1234};
  auto literal_tuple_entry = read_tuples.add_literal_value(value);
  auto parameter_tuple_entry = read_tuples.add_parameter(DataType::Double, ParameterID{1});

  // Create filter expression
  // clang-format off
  auto literal_expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                            JitExpressionType::LessThanEquals,
                                                            std::make_shared<JitExpression>(literal_tuple_entry, value),
                                                            read_tuples.add_temporary_value());
  auto parameter_expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(b_tuple_entry),
                                                              JitExpressionType::GreaterThanEquals,
                                                              std::make_shared<JitExpression>(parameter_tuple_entry),
                                                              read_tuples.add_temporary_value());
  // clang-format on
  read_tuples.add_value_id_expression(literal_expression);
  read_tuples.add_value_id_expression(parameter_expression);

  JitRuntimeContext context;
  context.tuple.resize(5);
  std::vector<AllTypeVariant> parameters{AllTypeVariant{0.5}};

  // Column b is unencoded in chunks 1 and 2 -> specialized function cannot be used for these chunks

  // Column a is unencoded in chunk 1 -> use actual values in comparison expression
  read_tuples.before_chunk(*input_table, ChunkID{1}, parameters, context);
  ASSERT_FALSE(literal_expression->use_value_ids);

  ASSERT_FALSE(parameter_expression->use_value_ids);

  // Column a is dicitonary-encoded in chunk 1 -> use value ids in comparison expression
  read_tuples.before_chunk(*input_table, ChunkID{2}, parameters, context);
  ASSERT_TRUE(literal_expression->use_value_ids);
}

TEST_F(JitReadWriteTupleTest, BeforeChunkCanUseSpecializedFunction) {
  // Check that the before_chunk() functiomn correctly specifies whether the specialized function can be used

  // Prepare input table
  auto input_table = load_table("resources/test_data/tbl/int.tbl", 1);
  ChunkEncoder::encode_chunks(input_table, {ChunkID{0}, ChunkID{2}});

  // Create JitReadTuples operator and JitExpressions
  JitReadTuples read_tuples;
  auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0});

  // Create filter expression
  // clang-format off
  auto expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                    JitExpressionType::IsNull,
                                                    read_tuples.add_temporary_value());
  // clang-format on
  read_tuples.add_value_id_expression(expression);

  JitRuntimeContext context;
  context.tuple.resize(3);

  std::vector<AllTypeVariant> parameters;

  // Chunk is dictionary-encoded
  bool use_spec_function_for_chunk_0 = read_tuples.before_chunk(*input_table, ChunkID{0}, parameters, context);
  ASSERT_TRUE(use_spec_function_for_chunk_0);

  // Chunk is unencoded
  bool use_spec_function_for_chunk_1 = read_tuples.before_chunk(*input_table, ChunkID{1}, parameters, context);
  ASSERT_FALSE(use_spec_function_for_chunk_1);

  // Chunk is dictionary-encoded
  bool use_spec_function_for_chunk_2 = read_tuples.before_chunk(*input_table, ChunkID{2}, parameters, context);
  ASSERT_TRUE(use_spec_function_for_chunk_2);
}

TEST_F(JitReadWriteTupleTest, UseValueIDsFromReferenceSegment) {
  // Correctly create iterators from referenced dictionary-encoded segments

  auto encoded_table = load_table("resources/test_data/tbl/int.tbl");
  ChunkEncoder::encode_all_chunks(encoded_table);

  // Create reference input table
  auto input_table = std::make_shared<Table>(encoded_table->column_definitions(), TableType::References);
  auto pos_list = std::make_shared<PosList>();
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{1}});
  pos_list->guarantee_single_chunk();
  Segments segments;
  segments.push_back(std::make_shared<ReferenceSegment>(encoded_table, ColumnID{0}, pos_list));
  input_table->append_chunk(segments);

  // Create JitReadTuples operator and JitExpressions
  JitReadTuples read_tuples;
  bool use_actual_value{false};
  auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, true, ColumnID{0}, use_actual_value);
  AllTypeVariant value{int32_t{4321}};
  auto literal_a_tuple_entry = read_tuples.add_literal_value(value);
  auto literal_b_tuple_entry = read_tuples.add_literal_value(value);

  // clang-format off
  auto expression_a = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                      JitExpressionType::LessThan,
                                                      std::make_shared<JitExpression>(literal_a_tuple_entry, value),
                                                      read_tuples.add_temporary_value());
  auto expression_b = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                      JitExpressionType::NotEquals,
                                                      std::make_shared<JitExpression>(literal_b_tuple_entry, value),
                                                      read_tuples.add_temporary_value());
  // clang-format off
  read_tuples.add_value_id_expression(expression_a);
  read_tuples.add_value_id_expression(expression_b);

  read_tuples.set_next_operator(std::make_shared<JitWriteTuples>());

  JitRuntimeContext context;
  std::vector<bool> tuple_non_nullable_information;
  read_tuples.before_specialization(*input_table, tuple_non_nullable_information);
  ASSERT_EQ(read_tuples.value_id_expressions().size(), 2u);
  read_tuples.before_query(*input_table, std::vector<AllTypeVariant>{}, context);
  read_tuples.before_chunk(*input_table, ChunkID{0}, std::vector<AllTypeVariant>{}, context);
  read_tuples.execute(context);

  // Used dictionary: 123, 1234, 12345
  // Segment value = 1234 -> value id = 1
  ASSERT_EQ(a_tuple_entry.get<ValueID>(context), ValueID{1});
  // a < 4321 -> value id = 2
  ASSERT_EQ(literal_a_tuple_entry.get<ValueID>(context), ValueID{2});
  // a != 4321 -> value id = INVALID_VALUE_ID
  ASSERT_EQ(literal_b_tuple_entry.get<ValueID>(context), INVALID_VALUE_ID);
}

TEST_F(JitReadWriteTupleTest, ReadActualValueAndValueIDFromColumn) {
  // Ensure that actual values and value ids are only read when needed

  auto input_table = load_table("resources/test_data/tbl/int.tbl");
  ChunkEncoder::encode_all_chunks(input_table);

  // Check which values from the last row are set in the runtime tuple for int value 12345 with value id 2

  {
    // Load actual value but not value id
    JitReadTuples read_tuples;
    const auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0});
    read_tuples.set_next_operator(std::make_shared<JitWriteTuples>());

    JitRuntimeContext context;
    read_tuples.before_query(*input_table, std::vector<AllTypeVariant>{}, context);
    read_tuples.before_chunk(*input_table, ChunkID{0}, std::vector<AllTypeVariant>{}, context);

    // Set value id to ensure it is not overwritten
    a_tuple_entry.set<ValueID>(ValueID{123456789}, context);

    read_tuples.execute(context);
    // Check that only the actual value is set
    ASSERT_EQ(a_tuple_entry.get<int32_t>(context), 12345);
    ASSERT_EQ(a_tuple_entry.get<ValueID>(context), ValueID{123456789});
  }

  {
    // Load value id but not actual value
    JitReadTuples read_tuples;
    bool use_actual_value{false};
    const auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0}, use_actual_value);
    // clang-format off
    auto expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                      JitExpressionType::IsNull,
                                                      read_tuples.add_temporary_value());
    // clang-format off
    read_tuples.add_value_id_expression(expression);
    read_tuples.set_next_operator(std::make_shared<JitWriteTuples>());

    JitRuntimeContext context;
    read_tuples.before_query(*input_table, std::vector<AllTypeVariant>{}, context);
    read_tuples.before_chunk(*input_table, ChunkID{0}, std::vector<AllTypeVariant>{}, context);

    // Set actual value to ensure it is not overwritten
    a_tuple_entry.set<int32_t>(123456789, context);

    read_tuples.execute(context);
    // Check that only the value id is set
    ASSERT_EQ(a_tuple_entry.get<int32_t>(context), 123456789);
    ASSERT_EQ(a_tuple_entry.get<ValueID>(context), ValueID{2});
  }

  {
    // Load actual value and value id
    JitReadTuples read_tuples;
    read_tuples.add_input_column(DataType::Int, false, ColumnID{0});
    bool use_actual_value{false};
    const auto a_tuple_entry = read_tuples.add_input_column(DataType::Int, false, ColumnID{0}, use_actual_value);
    // clang-format off
    auto expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                      JitExpressionType::IsNull,
                                                      read_tuples.add_temporary_value());
    // clang-format off
    read_tuples.add_value_id_expression(expression);
    read_tuples.set_next_operator(std::make_shared<JitWriteTuples>());

    JitRuntimeContext context;
    read_tuples.before_query(*input_table, std::vector<AllTypeVariant>{}, context);
    read_tuples.before_chunk(*input_table, ChunkID{0}, std::vector<AllTypeVariant>{}, context);

    read_tuples.execute(context);
    // Check that actual value and value id are set
    ASSERT_EQ(a_tuple_entry.get<int32_t>(context), 12345);
    ASSERT_EQ(a_tuple_entry.get<ValueID>(context), ValueID{2});
  }
}

TEST_F(JitReadWriteTupleTest, UpdateNullableInformationBeforeSpecialization) {
  // The information whether a column is nullable is set in before_specialization

  JitReadTuples read_tuples;
  JitWriteTuples write_tuples;

  auto tuple_entry_a = read_tuples.add_input_column(DataType::Int, false, ColumnID{0});
  auto tuple_entry_b = read_tuples.add_input_column(DataType::Long, false, ColumnID{1});

  write_tuples.add_output_column_definition("a", tuple_entry_a);
  write_tuples.add_output_column_definition("b", tuple_entry_b);

  auto& input_columns = read_tuples.input_columns();
  ASSERT_FALSE(input_columns[0].tuple_entry.guaranteed_non_null);
  ASSERT_FALSE(input_columns[1].tuple_entry.guaranteed_non_null);

  TableColumnDefinitions column_definitions = {{"a", DataType::Int,  false},
                                               {"b", DataType::Long, true}};
  auto input_table = Table::create_dummy_table(column_definitions);

  // Update nullable information of result entries for input column values
  std::vector<bool> tuple_non_nullable_information;
  read_tuples.before_specialization(*input_table, tuple_non_nullable_information);
  write_tuples.before_specialization(*input_table, tuple_non_nullable_information);

  // Nullable information is updated in the result entries ...
  ASSERT_TRUE(input_columns[0].tuple_entry.guaranteed_non_null);
  ASSERT_FALSE(input_columns[1].tuple_entry.guaranteed_non_null);

  auto output_table = write_tuples.create_output_table(*input_table);
  ASSERT_EQ(output_table->column_definitions(), column_definitions);

  // ... and the tuple_non_nullable_information vector
  EXPECT_TRUE(tuple_non_nullable_information[0]);
  EXPECT_FALSE(tuple_non_nullable_information[1]);
}

}  // namespace opossum
