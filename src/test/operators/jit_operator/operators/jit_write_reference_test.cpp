#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_write_references.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class JitWriteReferenceTest : public BaseTest {
 protected:
  Segments create_reference_segments(std::shared_ptr<Table> referenced_table, const bool same_pos_list,
                                     const bool guarantee_single_chunk, const ChunkID chunk_id) const {
    auto pos_list_1 = std::make_shared<PosList>();
    pos_list_1->emplace_back(RowID{chunk_id, ChunkOffset{0}});

    auto pos_list_2 = pos_list_1;
    if (!same_pos_list) {
      pos_list_2 = std::make_shared<PosList>();
      pos_list_2->emplace_back(RowID{chunk_id, ChunkOffset{0}});
    }

    if (guarantee_single_chunk) {
      pos_list_1->guarantee_single_chunk();
      pos_list_2->guarantee_single_chunk();
    }

    Segments segments;
    segments.push_back(std::make_shared<ReferenceSegment>(referenced_table, ColumnID{0}, pos_list_1));
    segments.push_back(std::make_shared<ReferenceSegment>(referenced_table, ColumnID{1}, pos_list_2));

    return segments;
  }
};

TEST_F(JitWriteReferenceTest, CreateOutputTable) {
  auto jit_write_references = std::make_shared<JitWriteReferences>();

  TableColumnDefinitions column_definitions = {{"a", DataType::Int, false},
                                               {"b", DataType::Long, true},
                                               {"c", DataType::Float, false},
                                               {"d", DataType::Double, false},
                                               {"e", DataType::String, true}};

  for (ColumnID column_id{0}; column_id < column_definitions.size(); ++column_id) {
    jit_write_references->add_output_column_definition(column_definitions[column_id].name, column_id);
  }

  auto output_table = jit_write_references->create_output_table(Table{column_definitions, TableType::Data});
  ASSERT_EQ(output_table->column_definitions(), column_definitions);
}

TEST_F(JitWriteReferenceTest, ConsumeTuple) {
  RowID row_id{ChunkID{3}, ChunkOffset{0}};

  JitRuntimeContext context;
  context.output_pos_list = std::make_shared<PosList>();
  context.chunk_id = row_id.chunk_id;
  context.chunk_offset = row_id.chunk_offset;
  context.chunk_size = 1;

  auto input = std::make_shared<JitReadTuples>();
  auto jit_write_references = std::make_shared<JitWriteReferences>();
  input->set_next_operator(jit_write_references);
  input->execute(context);

  ASSERT_EQ(context.output_pos_list->size(), 1u);
  ASSERT_EQ((*context.output_pos_list)[0], row_id);
}

TEST_F(JitWriteReferenceTest, AfterChunkDataInputTable) {
  JitRuntimeContext context;
  context.output_pos_list = std::make_shared<PosList>();

  JitWriteReferences jit_write_references;

  // Add all input table columns to pipeline
  jit_write_references.add_output_column_definition("a", ColumnID{0});
  jit_write_references.add_output_column_definition("b", ColumnID{1});

  // Create input reference table
  auto input_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);

  auto output_table = jit_write_references.create_output_table(*input_table);
  jit_write_references.before_query(*output_table, context);

  // Add row to output
  context.output_pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});

  jit_write_references.after_chunk(input_table, *output_table, context);

  // Chunk contains one row.
  const auto output_chunk = output_table->get_chunk(ChunkID{0});
  ASSERT_EQ(output_chunk->size(), 1u);

  // PosLists of both segments are the same
  auto reference_segment_a = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{0}));
  auto reference_segment_b = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{1}));
  ASSERT_EQ(reference_segment_a->pos_list(), reference_segment_b->pos_list());

  // PosList guarantees single chunk
  ASSERT_TRUE(reference_segment_a->pos_list()->references_single_chunk());
}

TEST_F(JitWriteReferenceTest, AfterChunkReferenceTableInputSamePosList) {
  JitRuntimeContext context;
  context.output_pos_list = std::make_shared<PosList>();

  JitWriteReferences jit_write_references;

  // Add all input table columns to pipeline
  jit_write_references.add_output_column_definition("a", ColumnID{0});
  jit_write_references.add_output_column_definition("b", ColumnID{1});

  // Create input reference table
  auto original_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);
  auto input_table = std::make_shared<Table>(original_table->column_definitions(), TableType::References);
  input_table->append_chunk(create_reference_segments(original_table, true, false, ChunkID{0}));
  input_table->append_chunk(create_reference_segments(original_table, true, true, ChunkID{1}));

  auto output_table = jit_write_references.create_output_table(*input_table);
  jit_write_references.before_query(*output_table, context);

  // No single chunk guarantee
  {
    // Add row to output
    context.output_pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});

    context.chunk_id = 0;
    jit_write_references.after_chunk(input_table, *output_table, context);

    // Chunk contains one row.
    const auto output_chunk = output_table->get_chunk(ChunkID{0});
    ASSERT_EQ(output_chunk->size(), 1u);

    // PosLists are same
    auto reference_segment_a = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{0}));
    auto reference_segment_b = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{1}));
    ASSERT_EQ(reference_segment_a->pos_list(), reference_segment_b->pos_list());

    // PosList does not guarantees single chunk
    ASSERT_FALSE(reference_segment_a->pos_list()->references_single_chunk());
  }

  // Single chunk guarantee is set
  {
    // Add row to output
    context.output_pos_list->emplace_back(RowID{ChunkID{1}, ChunkOffset{0}});

    context.chunk_id = 1;
    jit_write_references.after_chunk(input_table, *output_table, context);

    // PosList guarantees single chunk
    const auto output_chunk = output_table->get_chunk(ChunkID{1});
    auto reference_segment_a = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{0}));
    ASSERT_TRUE(reference_segment_a->pos_list()->references_single_chunk());
  }

  // Input and output table should be equal
  EXPECT_TABLE_EQ_ORDERED(input_table, output_table);
}

TEST_F(JitWriteReferenceTest, AfterChunkReferenceTableInputDifferentPosLists) {
  JitRuntimeContext context;
  context.output_pos_list = std::make_shared<PosList>();

  JitWriteReferences jit_write_references;

  // Add all input table columns to pipeline
  jit_write_references.add_output_column_definition("a", ColumnID{0});
  jit_write_references.add_output_column_definition("b", ColumnID{1});

  // Create input reference table
  auto original_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);
  auto input_table = std::make_shared<Table>(original_table->column_definitions(), TableType::References);
  input_table->append_chunk(create_reference_segments(original_table, false, false, ChunkID{0}));
  input_table->append_chunk(create_reference_segments(original_table, false, true, ChunkID{1}));

  auto output_table = jit_write_references.create_output_table(*input_table);
  jit_write_references.before_query(*output_table, context);

  // No single chunk guarantee
  {
    // Add row to output
    context.output_pos_list->emplace_back(RowID{ChunkID{0}, ChunkOffset{0}});

    context.chunk_id = 0;
    jit_write_references.after_chunk(input_table, *output_table, context);

    // Chunk contains one row.
    const auto output_chunk = output_table->get_chunk(ChunkID{0});
    ASSERT_EQ(output_chunk->size(), 1u);

    // PosLists are different
    auto reference_segment_a = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{0}));
    auto reference_segment_b = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{1}));
    ASSERT_NE(reference_segment_a->pos_list(), reference_segment_b->pos_list());

    // PosList does not guarantee single chunk
    ASSERT_FALSE(reference_segment_a->pos_list()->references_single_chunk());
  }

  // Single chunk guarantee is set
  {
    // Add row to output
    context.output_pos_list->emplace_back(RowID{ChunkID{1}, ChunkOffset{0}});

    context.chunk_id = 1;
    jit_write_references.after_chunk(input_table, *output_table, context);

    // PosList guarantees single chunk
    const auto output_chunk = output_table->get_chunk(ChunkID{1});
    auto reference_segment_a = std::dynamic_pointer_cast<ReferenceSegment>(output_chunk->get_segment(ColumnID{0}));
    ASSERT_TRUE(reference_segment_a->pos_list()->references_single_chunk());
  }

  // Input and output table should be equal
  EXPECT_TABLE_EQ_ORDERED(input_table, output_table);
}

TEST_F(JitWriteReferenceTest, CopyDataTable) {
  JitRuntimeContext context;

  // Create operator chain that passes from the input tuple to an output table unmodified
  auto read_tuples = std::make_shared<JitReadTuples>();
  auto jit_write_references = std::make_shared<JitWriteReferences>();
  read_tuples->set_next_operator(jit_write_references);

  // Add all input table columns to pipeline
  jit_write_references->add_output_column_definition("a", ColumnID{0});
  jit_write_references->add_output_column_definition("b", ColumnID{1});

  // Initialize operators with actual input table
  auto input_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);
  auto output_table = jit_write_references->create_output_table(*input_table);
  read_tuples->before_query(*input_table, std::vector<AllTypeVariant>(), context);
  jit_write_references->before_query(*output_table, context);

  // Pass each chunk through the pipeline
  for (ChunkID chunk_id{0}; chunk_id < 2u; ++chunk_id) {
    read_tuples->before_chunk(*input_table, chunk_id, std::vector<AllTypeVariant>(), context);
    read_tuples->execute(context);
    jit_write_references->after_chunk(input_table, *output_table, context);
  }
  jit_write_references->after_query(*output_table, context);

  // Input and output table should be equal
  EXPECT_TABLE_EQ_ORDERED(input_table, output_table);
}

}  // namespace opossum
