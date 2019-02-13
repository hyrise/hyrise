#include "table_sample.hpp"

#include <string>

#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"

namespace opossum {

TableSample::TableSample(const std::shared_ptr<const AbstractOperator>& in, const size_t num_rows):
  AbstractReadOnlyOperator(OperatorType::TableSample, in), _num_rows(num_rows) {

}

const std::string TableSample::name() const {
  return "TableSample";
}

const std::string TableSample::description(DescriptionMode description_mode) const {
  return name() + ": " + std::to_string(_num_rows);
}

Segments TableSample::filter_chunk_with_pos_list(const std::shared_ptr<const Table>& table, const Chunk& chunk, const std::shared_ptr<PosList>& pos_list) {
  Segments out_segments;

  /**
   * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
   * directly use the matches to construct the reference segments of the output. If it is a reference segment,
   * we need to resolve the row IDs so that they reference the physical data segments (value, dictionary) instead,
   * since we don’t allow multi-level referencing. To save time and space, we want to share position lists
   * between segments as much as possible. Position lists can be shared between two segments iff
   * (a) they point to the same table and
   * (b) the reference segments of the input table point to the same positions in the same order
   *     (i.e. they share their position list).
   */
  if (table->type() == TableType::References) {
    auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

    for (ColumnID column_id{0u}; column_id < table->column_count(); ++column_id) {
      auto segment_in = chunk.get_segment(column_id);

      auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
      DebugAssert(ref_segment_in != nullptr, "All segments should be of type ReferenceSegment.");

      const auto pos_list_in = ref_segment_in->pos_list();

      const auto table_out = ref_segment_in->referenced_table();
      const auto column_id_out = ref_segment_in->referenced_column_id();

      auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

      if (!filtered_pos_list) {
        filtered_pos_list = std::make_shared<PosList>(pos_list->size());
        if (pos_list_in->references_single_chunk()) {
          filtered_pos_list->guarantee_single_chunk();
        }

        size_t offset = 0;
        for (const auto& match : *pos_list) {
          const auto row_id = (*pos_list_in)[match.chunk_offset];
          (*filtered_pos_list)[offset] = row_id;
          ++offset;
        }
      }

      auto ref_segment_out = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
      out_segments.push_back(ref_segment_out);
    }
  } else {
    pos_list->guarantee_single_chunk();
    for (ColumnID column_id{0u}; column_id < table->column_count(); ++column_id) {
      auto ref_segment_out = std::make_shared<ReferenceSegment>(table, column_id, pos_list);
      out_segments.push_back(ref_segment_out);
    }
  }

  return out_segments;
}

std::shared_ptr<const Table> TableSample::_on_execute() {
  if (_num_rows >= input_table_left()->row_count()) {
    return input_table_left();
  }

  const auto output_table = std::make_shared<Table>(input_table_left()->column_definitions(), TableType::References, std::nullopt, UseMvcc::No);

  if (_num_rows == 0) {
    return output_table;
  }

  const auto step = input_table_left()->row_count() / _num_rows;
  const auto remainder = input_table_left()->row_count() % _num_rows;

  auto chunk_offset = size_t{step - 1};
  // Magic variable that gets increased by `remainder` per step
  auto k = size_t{0};

  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    const auto& chunk = input_table_left()->get_chunk(chunk_id);
    auto pos_list = std::make_shared<PosList>();

    const auto chunk_size = chunk->size();
    for(; chunk_offset < chunk_size; ) {
      pos_list->emplace_back(chunk_id, chunk_offset);

      chunk_offset += step;
      k += remainder;

      if (k >= _num_rows) {
        ++chunk_offset;
        k -= _num_rows;
      }
    }

    chunk_offset -= chunk_size;

    output_table->append_chunk(filter_chunk_with_pos_list(input_table_left(), *chunk, pos_list));
  }

  return output_table;
}

std::shared_ptr<AbstractOperator> TableSample::_on_deep_copy(
const std::shared_ptr<AbstractOperator>& copied_input_left,
const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<TableSample>(copied_input_left, _num_rows);
}

void TableSample::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No params
}

}  // namespace opossum