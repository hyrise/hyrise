#include "row_sort.hpp"

#include <cstring>

#include "storage/chunk.hpp"
#include "utils/key_normalizer.h"

namespace hyrise {

void get_normalized_modes(const SortMode hyrise_mode, NormalizedSortMode& normalized_sort_mode, NullsMode& nulls_mode) {
  switch (hyrise_mode) {
    case SortMode::AscendingNullsFirst:
      normalized_sort_mode = NormalizedSortMode::Ascending;
      nulls_mode = NullsMode::NullsFirst;
      break;
    case SortMode::AscendingNullsLast:
      normalized_sort_mode = NormalizedSortMode::Ascending;
      nulls_mode = NullsMode::NullsLast;
      break;
    case SortMode::DescendingNullsFirst:
      normalized_sort_mode = NormalizedSortMode::Descending;
      nulls_mode = NullsMode::NullsFirst;
      break;
    case SortMode::DescendingNullsLast:
      normalized_sort_mode = NormalizedSortMode::Descending;
      nulls_mode = NullsMode::NullsLast;
      break;
  }
}

RowSort::RowSort(const std::shared_ptr<const AbstractOperator>& input,
                 const std::vector<SortColumnDefinition>& sort_definitions, const ChunkOffset output_chunk_size,
                 const Sort::ForceMaterialization force_materialization)
    : AbstractReadOnlyOperator(OperatorType::Sort, input),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std::vector<SortColumnDefinition>& RowSort::sort_definitions() const {
  return _sort_definitions;
}

const std::string& RowSort::name() const {
  static const auto name = std::string{"RowSort"};
  return name;
}

std::shared_ptr<AbstractOperator> RowSort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<RowSort>(copied_left_input, _sort_definitions);
}

void RowSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> RowSort::_on_execute() {
  const auto input_table = left_input_table();

  if (input_table->row_count() == 0) {
    if (_force_materialization == Sort::ForceMaterialization::Yes) {
      return Table::create_dummy_table(input_table->column_definitions());
    }
    return input_table;
  }

  const auto [normalized_keys, tuple_key_size] = KeyNormalizer::convert_table(input_table, _sort_definitions);

  auto [iterator, end_iterator] = KeyNormalizer::get_iterators(normalized_keys, tuple_key_size);
  std::vector<RowIdIterator> sort_iterators;
  sort_iterators.reserve(input_table->row_count());
  for (; iterator != end_iterator; ++iterator) {
    sort_iterators.push_back(iterator);
  }

  std::stable_sort(sort_iterators.begin(), sort_iterators.end(),
                   [tuple_key_size](const RowIdIterator& a, const RowIdIterator& b) {
                     return std::memcmp(a.operator->(), b.operator->(), tuple_key_size) < 0;
                   });

  auto pos_list = std::make_shared<RowIDPosList>();
  pos_list->reserve(sort_iterators.size());
  for (const auto& sorted_iterator : sort_iterators) {
    pos_list->emplace_back(*sorted_iterator);
  }

  std::shared_ptr<Table> output_table;

  if (_force_materialization == Sort::ForceMaterialization::Yes) {
    output_table =
        std::make_shared<Table>(input_table->column_definitions(), TableType::Data, _output_chunk_size, UseMvcc::Yes);
    for (const auto& row_id : *pos_list) {
      const auto& chunk = input_table->get_chunk(row_id.chunk_id);
      if (!chunk) {
        continue;
      }
      std::vector<AllTypeVariant> row_values;
      row_values.reserve(input_table->column_count());
      for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
        row_values.emplace_back((*chunk->get_segment(column_id))[row_id.chunk_offset]);
      }
      output_table->append(row_values);
    }
  } else {
    output_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References);
    Segments output_segments;
    output_segments.reserve(input_table->column_count());
    for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
      output_segments.emplace_back(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }
    output_table->append_chunk(output_segments);
  }

  const auto& final_sort_definition = _sort_definitions[0];
  const auto output_chunk_count = output_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < output_chunk_count; ++chunk_id) {
    const auto& chunk = output_table->get_chunk(chunk_id);
    chunk->set_immutable();
    chunk->set_individually_sorted_by(final_sort_definition);
  }

  return output_table;
}

}  // namespace hyrise