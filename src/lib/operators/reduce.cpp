#include "reduce.hpp"

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"

namespace hyrise {

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
Reduce<Hasher, FilterSize, K>::Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                                   const std::shared_ptr<const AbstractOperator>& right_input,
                                   const OperatorJoinPredicate predicate, const bool update_filter)
    : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input},
      _predicate(predicate),
      _update_filter(update_filter) {}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
void Reduce<Hasher, FilterSize, K>::_create_filter(const std::shared_ptr<const Table>& table, const ColumnID column_id) {
  const auto FILTER_SIZE = static_cast<uint32_t>(pow(2u, FilterSize));

  Assert(FILTER_SIZE % 64 == 0, "Filter size must be a multiple of 64.");
  _filter = std::make_shared<std::vector<std::atomic_uint64_t>>(FILTER_SIZE / 64);

  const auto chunk_count = table->chunk_count();

  resolve_data_type(table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = Hasher<ColumnDataType>{};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& segment = table->get_chunk(chunk_index)->get_segment(column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          _set_bit(hasher(static_cast<ColumnDataType>(position.value())));
        }
      });
    }
  });
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
std::shared_ptr<Table> Reduce<Hasher, FilterSize, K>::_create_reduced_table() {
  Assert(_filter, "Can not filter without filter.");

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();
  const auto column_id = _predicate.column_ids.first;

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(chunk_count);

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = Hasher<ColumnDataType>{};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& chunk = input_table->get_chunk(chunk_index);
      const auto& segment = chunk->get_segment(column_id);
      auto matches = std::make_shared<RowIDPosList>();
      matches->guarantee_single_chunk();

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          bool true0 = true;
          true0 = _get_bit(hasher(static_cast<ColumnDataType>(position.value())));

          if (true0) {
            matches->emplace_back(RowID{chunk_index, position.chunk_offset()});
          }
        }
      });

      if (!matches->empty()) {
        const auto column_count = input_table->column_count();
        auto out_segments = Segments{};
        out_segments.reserve(column_count);

        auto keep_chunk_sort_order = true;
        if (input_table->type() == TableType::References) {
          if (matches->size() == chunk->size()) {
            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
              const auto segment_in = chunk->get_segment(column_id);
              out_segments.emplace_back(segment_in);
            }
          } else {
            auto filtered_pos_lists = std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>>{};

            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
              const auto segment_in = chunk->get_segment(column_id);

              auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
              DebugAssert(ref_segment_in, "All segments should be of type ReferenceSegment.");

              const auto pos_list_in = ref_segment_in->pos_list();

              const auto table_out = ref_segment_in->referenced_table();
              const auto column_id_out = ref_segment_in->referenced_column_id();

              auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

              if (!filtered_pos_list) {
                filtered_pos_list = std::make_shared<RowIDPosList>(matches->size());
                if (pos_list_in->references_single_chunk()) {
                  filtered_pos_list->guarantee_single_chunk();
                } else {
                  keep_chunk_sort_order = false;
                }

                auto offset = size_t{0};
                for (const auto& match : *matches) {
                  const auto row_id = (*pos_list_in)[match.chunk_offset];
                  (*filtered_pos_list)[offset] = row_id;
                  ++offset;
                }
              }

              const auto ref_segment_out =
                  std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
              out_segments.push_back(ref_segment_out);
            }
          }
        } else {
          matches->guarantee_single_chunk();

          const auto output_pos_list = matches->size() == chunk->size()
                                           ? static_cast<std::shared_ptr<AbstractPosList>>(
                                                 std::make_shared<EntireChunkPosList>(chunk_index, chunk->size()))
                                           : static_cast<std::shared_ptr<AbstractPosList>>(matches);

          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            const auto ref_segment_out = std::make_shared<ReferenceSegment>(input_table, column_id, output_pos_list);
            out_segments.push_back(ref_segment_out);
          }
        }

        const auto out_chunk = std::make_shared<Chunk>(out_segments, nullptr, chunk->get_allocator());
        out_chunk->set_immutable();
        if (keep_chunk_sort_order && !chunk->individually_sorted_by().empty()) {
          out_chunk->set_individually_sorted_by(chunk->individually_sorted_by());
        }
        output_chunks.emplace_back(out_chunk);
      }
    }
  });

  const auto output_table =
      std::make_shared<Table>(input_table->column_definitions(), TableType::References, std::move(output_chunks));

  auto file_exists = std::filesystem::exists("reduction_stats.csv");
  std::ofstream output_file;
  output_file.open("reduction_stats.csv", std::ios_base::app);

  if (!file_exists) {
    output_file << "filter_size,hash_count,benchmark,query,input_count,output_count\n";
  }
  output_file << _filter_size_string << "," << _hash_count << "," << Hyrise::get().benchmark_name << ","
              << Hyrise::get().query_name << "," << input_table->row_count() << "," << output_table->row_count()
              << "\n";

  return output_table;
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
const std::string& Reduce<Hasher, FilterSize, K>::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
const std::shared_ptr<std::vector<std::atomic_uint64_t>>& Reduce<Hasher, FilterSize, K>::export_filter() const {
  Assert(_filter, "Filter is not set, reducer was probably not executed.");
  return _filter;
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
std::shared_ptr<const Table> Reduce<Hasher, FilterSize, K>::_on_execute() {
  if (_right_input->type() == OperatorType::Reduce) {
    const auto input_reducer = std::static_pointer_cast<const Reduce>(_right_input);
    _filter = input_reducer->export_filter();
  } else {
    _create_filter(_right_input->get_output(), _predicate.column_ids.second);
  }
  const auto output_table = _create_reduced_table();

  if (_update_filter) {
    _filter = nullptr;
    _create_filter(output_table, _predicate.column_ids.first);
  }

  return output_table;
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
void Reduce<Hasher, FilterSize, K>::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
std::shared_ptr<AbstractOperator> Reduce<Hasher, FilterSize, K>::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Reduce>(copied_left_input, copied_right_input, _predicate, _update_filter);
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
void Reduce<Hasher, FilterSize, K>::_set_bit(const uint32_t hash_22bit) {
  size_t index = hash_22bit / 64;   // Determine the block
  size_t offset = hash_22bit % 64;  // Determine the bit within the block
  (*_filter)[index].fetch_or(1ULL << offset, std::memory_order_relaxed);
}

template <template <typename> class Hasher, uint8_t FilterSize, uint8_t K>
bool Reduce<Hasher, FilterSize, K>::_get_bit(const uint32_t hash_22bit) const {
  size_t index = hash_22bit / 64;   // Determine the block
  size_t offset = hash_22bit % 64;  // Determine the bit within the block
  return ((*_filter)[index].load(std::memory_order_relaxed) >> offset) & 1;
}

template class Reduce<std::hash, 20>;

}  // namespace hyrise
