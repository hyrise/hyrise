#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "null_value.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

namespace {

const auto DEFINITIONS = TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::String, true}};

void append_row(OutputColumns& output, const int32_t int_value, const std::optional<pmr_string>& string_value) {
  static_cast<TypedOutputColumn<int32_t>&>(output.column(0)).append(int_value);
  auto& string_column = static_cast<TypedOutputColumn<pmr_string>&>(output.column(1));
  if (string_value) {
    string_column.append(*string_value);
  } else {
    string_column.append_null();
  }
}

void expect_segment_values(const std::shared_ptr<AbstractSegment>& segment,
                           const std::vector<AllTypeVariant>& expected) {
  ASSERT_EQ(segment->size(), expected.size());
  for (auto row = size_t{0}; row < expected.size(); ++row) {
    const auto actual = (*segment)[ChunkOffset{static_cast<uint32_t>(row)}];
    if (variant_is_null(expected[row])) {
      EXPECT_TRUE(variant_is_null(actual)) << "row " << row;
    } else {
      EXPECT_EQ(actual, expected[row]) << "row " << row;
    }
  }
}

}  // namespace

class AggregateDYODOutputColumnsTest : public BaseTest {};

TEST_F(AggregateDYODOutputColumnsTest, MaybeSealDoesNothingBelowThreshold) {
  auto output = OutputColumns{DEFINITIONS, /*seal_threshold=*/4};
  append_row(output, 1, pmr_string{"x"});
  append_row(output, 2, std::nullopt);
  append_row(output, 3, pmr_string{"y"});

  output.maybe_seal();

  EXPECT_EQ(output.column(0).sealed_chunk_count(), 0u);
  EXPECT_EQ(output.column(1).sealed_chunk_count(), 0u);
  EXPECT_EQ(output.column(0).in_progress_row_count(), 3u);
  EXPECT_EQ(output.column(1).in_progress_row_count(), 3u);
}

TEST_F(AggregateDYODOutputColumnsTest, MaybeSealCutsChunkAtThreshold) {
  auto output = OutputColumns{DEFINITIONS, /*seal_threshold=*/2};
  append_row(output, 1, pmr_string{"x"});
  append_row(output, 2, pmr_string{"y"});

  output.maybe_seal();

  EXPECT_EQ(output.column(0).sealed_chunk_count(), 1u);
  EXPECT_EQ(output.column(1).sealed_chunk_count(), 1u);
  EXPECT_EQ(output.column(0).in_progress_row_count(), 0u);
  EXPECT_EQ(output.column(1).in_progress_row_count(), 0u);
}

TEST_F(AggregateDYODOutputColumnsTest, SealingTwiceKeepsChunksAlignedAndOrdered) {
  auto output = OutputColumns{DEFINITIONS, /*seal_threshold=*/2};
  append_row(output, 1, pmr_string{"one"});
  append_row(output, 2, std::nullopt);
  output.maybe_seal();
  append_row(output, 3, pmr_string{"three"});
  output.maybe_seal();  // Below the threshold, must not cut.
  output.seal_all();

  const auto int_segments = output.column(0).take_segments();
  const auto string_segments = output.column(1).take_segments();
  ASSERT_EQ(int_segments.size(), 2u);
  ASSERT_EQ(string_segments.size(), 2u);

  expect_segment_values(int_segments[0], {1, 2});
  expect_segment_values(int_segments[1], {3});
  expect_segment_values(string_segments[0], {pmr_string{"one"}, NullValue{}});
  expect_segment_values(string_segments[1], {pmr_string{"three"}});
}

TEST_F(AggregateDYODOutputColumnsTest, BuildOutputTableStitchesWorkerChunksInOrder) {
  auto per_worker_outputs = std::vector<OutputColumns>{};
  per_worker_outputs.emplace_back(DEFINITIONS, 2);
  per_worker_outputs.emplace_back(DEFINITIONS, 2);

  append_row(per_worker_outputs[0], 1, pmr_string{"one"});
  append_row(per_worker_outputs[0], 2, std::nullopt);
  per_worker_outputs[0].maybe_seal();
  append_row(per_worker_outputs[0], 3, pmr_string{"three"});
  per_worker_outputs[0].seal_all();

  append_row(per_worker_outputs[1], 4, pmr_string{"four"});
  per_worker_outputs[1].seal_all();

  const auto table = build_output_table(DEFINITIONS, per_worker_outputs);

  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->column_definitions(), DEFINITIONS);
  ASSERT_EQ(table->chunk_count(), 3u);
  EXPECT_EQ(table->row_count(), 4u);

  expect_segment_values(table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), {1, 2});
  expect_segment_values(table->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), {pmr_string{"one"}, NullValue{}});
  expect_segment_values(table->get_chunk(ChunkID{1})->get_segment(ColumnID{0}), {3});
  expect_segment_values(table->get_chunk(ChunkID{1})->get_segment(ColumnID{1}), {pmr_string{"three"}});
  expect_segment_values(table->get_chunk(ChunkID{2})->get_segment(ColumnID{0}), {4});
  expect_segment_values(table->get_chunk(ChunkID{2})->get_segment(ColumnID{1}), {pmr_string{"four"}});
}

TEST_F(AggregateDYODOutputColumnsTest, BuildOutputTableWithoutRowsYieldsEmptyTable) {
  auto per_worker_outputs = std::vector<OutputColumns>{};
  per_worker_outputs.emplace_back(DEFINITIONS, 2);
  per_worker_outputs[0].seal_all();

  const auto table = build_output_table(DEFINITIONS, per_worker_outputs);

  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->chunk_count(), 0u);
  EXPECT_EQ(table->row_count(), 0u);
}

}  // namespace hyrise
