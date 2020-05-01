#include "base_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

struct SortTestParam {
  std::vector<SortColumnDefinition> sort_columns;

  bool input_is_empty;
  bool input_is_reference;
  ChunkOffset output_chunk_size;
  Sort::ForceMaterialization force_materialization;

  std::string expected_filename;
};

class SortTest : public BaseTestWithParam<SortTestParam> {
 public:
  static void SetUpTestCase() {
    input_table = load_table("resources/test_data/tbl/sort/input.tbl", 20);
    input_table_wrapper = std::make_shared<TableWrapper>(input_table);
    input_table_wrapper->execute();
  }

  static inline std::shared_ptr<Table> input_table;
  static inline std::shared_ptr<AbstractOperator> input_table_wrapper;
};

TEST_P(SortTest, Sort) {
  auto param = GetParam();

  auto input = input_table_wrapper;

  if (param.input_is_empty) {
    if (param.input_is_reference) {
      // Create an empty reference table
      input = std::make_shared<TableScan>(input, equals_(1, 2));
      input->execute();
    } else {
      // Create an empty data table
      auto empty_table = Table::create_dummy_table(input_table->column_definitions());
      input = std::make_shared<TableWrapper>(empty_table);
      input->execute();
    }
  }

  auto sort = Sort{input, param.sort_columns, param.output_chunk_size, param.force_materialization};
  sort.execute();

  const auto expected_table = load_table(std::string{"resources/test_data/tbl/sort/"} + param.expected_filename);
  const auto& result = sort.get_output();
  EXPECT_TABLE_EQ_ORDERED(result, expected_table);

  // Verify type of result table
  if (param.force_materialization == Sort::ForceMaterialization::Yes ||
      (param.input_is_empty && !param.input_is_reference)) {
    EXPECT_EQ(result->type(), TableType::Data);
  } else {
    EXPECT_EQ(result->type(), TableType::References);
  }

  // Verify output chunk size
  if (result->chunk_count() > 0) {
    for (auto chunk_id = ChunkID{0}; chunk_id < result->chunk_count() - 1; ++chunk_id) {
      EXPECT_EQ(result->get_chunk(chunk_id)->size(), param.output_chunk_size);
    }
  }
}

inline std::string sort_test_formatter(const testing::TestParamInfo<SortTestParam>& param_info) {
  const auto& param = param_info.param;

  std::stringstream stream;
  if (param.input_is_empty) stream << "Empty";
  stream << (param.input_is_reference ? "Reference" : "Data") << "Input";
  for (const auto& sort_column : param.sort_columns) {
    stream << "Col" << sort_column.column << sort_mode_to_string.left.at(sort_column.sort_mode);
  }

  if (param.output_chunk_size != Chunk::DEFAULT_SIZE) stream << "ChunkSize" << param.output_chunk_size;

  if (param.force_materialization == Sort::ForceMaterialization::Yes) stream << "ForcedMaterialization";

  return stream.str();
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(Variations, SortTest,
                         ::testing::Values(
                           // Variantions of different orders
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}},                                                                       false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "a_asc.tbl"},             // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Descending}},                                                                      false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "a_desc.tbl"},            // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "a_asc_b_desc.tbl"},      // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{1}, SortMode::AscendingNullsLast}, SortColumnDefinition{ColumnID{0}, SortMode::Descending}},  false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "b_asclast_a_asc.tbl"},   // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsLast}},  false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "a_asc_b_desclast.tbl"},  // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{2}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsLast}},  false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "c_asc_b_desclast.tbl"},  // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Descending}, SortColumnDefinition{ColumnID{1}, SortMode::Ascending}},           false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "a_desc_b_asc.tbl"},      // NOLINT

                           // Output chunk size
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, false, 40,                  Sort::ForceMaterialization::No,  "a_asc_b_desc.tbl"},      // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, true,  40,                  Sort::ForceMaterialization::No,  "a_asc_b_desc.tbl"},      // NOLINT

                           // Empty input tables
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}},                                                                       true,  false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "empty.tbl"},             // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}},                                                                       true,  true,  Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No,  "empty.tbl"},             // NOLINT

                           // Forced materialization
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes, "a_asc_b_desc.tbl"},      // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, false, 33,                  Sort::ForceMaterialization::Yes, "a_asc_b_desc.tbl"},      // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, true,  Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes, "a_asc_b_desc.tbl"},      // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}},           false, true,  33,                  Sort::ForceMaterialization::Yes, "a_asc_b_desc.tbl"},      // NOLINT

                           // Empty input tables with forced materialization
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}},                                                                       true,  false, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes, "empty.tbl"},             // NOLINT
                           SortTestParam{{SortColumnDefinition{ColumnID{0}, SortMode::Ascending}},                                                                       true,  true,  Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes, "empty.tbl"}              // NOLINT
                          ),  // NOLINT
                         sort_test_formatter);
// clang-format on

TEST_F(SortTest, JoinProducesReferences) {
  // Even though not all columns in a join result refer to the same table, the output should use references
  const auto right_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int3.tbl"));
  right_wrapper->execute();

  const auto join_predicate = OperatorJoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  auto join = std::make_shared<JoinHash>(input_table_wrapper, right_wrapper, JoinMode::Inner, join_predicate);
  join->execute();

  auto sort = Sort{join, {SortColumnDefinition{ColumnID{1}, SortMode::Descending}}};
  sort.execute();

  EXPECT_EQ(sort.get_output()->type(), TableType::References);
}

TEST_F(SortTest, InputReferencesDifferentTables) {
  // When a single column in a table references different tables, we cannot output sorted ReferenceSegments.
  // This test simulates the output of a union on the first column.

  const auto second_table = load_table("resources/test_data/tbl/sort/a_asc.tbl", 10);
  const auto second_table_wrapper = std::make_shared<TableWrapper>(second_table);
  second_table_wrapper->execute();

  const auto union_table = std::make_shared<Table>(
      TableColumnDefinitions{TableColumnDefinition{"a", DataType::Int, true}}, TableType::References);

  auto pos_list = std::make_shared<RowIDPosList>();
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{1}});
  pos_list->emplace_back(RowID{ChunkID{1}, ChunkOffset{0}});

  auto first_reference_segment = std::make_shared<ReferenceSegment>(input_table, ColumnID{0}, pos_list);
  union_table->append_chunk(Segments{first_reference_segment});

  auto second_reference_segment = std::make_shared<ReferenceSegment>(second_table, ColumnID{0}, pos_list);
  union_table->append_chunk(Segments{second_reference_segment});

  const auto union_table_wrapper = std::make_shared<TableWrapper>(union_table);
  union_table_wrapper->execute();

  auto sort = Sort{union_table_wrapper, {SortColumnDefinition{ColumnID{0}, SortMode::Descending}}};
  sort.execute();

  EXPECT_EQ(sort.get_output()->type(), TableType::Data);
}

TEST_F(SortTest, InputReferencesDifferentColumns) {
  // Similarly to InputReferencesDifferentTables, we cannot build a ReferenceSegment that references different columns
  // in the same table.

  // This is not just a normal union_table but something weird that you probably won't see in the wild
  const auto weird_table = std::make_shared<Table>(
      TableColumnDefinitions{TableColumnDefinition{"a", DataType::Int, true}}, TableType::References);

  auto pos_list = std::make_shared<RowIDPosList>();
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});
  pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{1}});
  pos_list->emplace_back(RowID{ChunkID{1}, ChunkOffset{0}});

  auto first_reference_segment = std::make_shared<ReferenceSegment>(input_table, ColumnID{0}, pos_list);
  weird_table->append_chunk(Segments{first_reference_segment});

  auto second_reference_segment = std::make_shared<ReferenceSegment>(input_table, ColumnID{1}, pos_list);
  weird_table->append_chunk(Segments{second_reference_segment});

  const auto weird_table_wrapper = std::make_shared<TableWrapper>(weird_table);
  weird_table_wrapper->execute();

  auto sort = Sort{weird_table_wrapper, {SortColumnDefinition{ColumnID{0}, SortMode::Descending}}};
  sort.execute();

  EXPECT_EQ(sort.get_output()->type(), TableType::Data);
}

}  // namespace opossum