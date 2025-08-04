#pragma once

#include <atomic>
#include <memory>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

#include "storage/segment_iterate.hpp"
#include "utils/bloom_filter.hpp"
#include "utils/min_max_filter.hpp"

namespace hyrise {

class Reduce : public AbstractReadOnlyOperator {

 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate)
                  : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input}, _predicate{predicate} {
    _bloom_filter = std::make_shared<BloomFilter<20, 2>>();
    _min_max_filter = std::make_shared<MinMaxFilter>();
  }

  const std::string& name() const override {
    static const auto name = std::string{"Reduce"};
    return name;
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    return nullptr;
  }

  void _create_filter(const std::shared_ptr<const Table>& table, const ColumnID column_id) {
    const auto chunk_count = table->chunk_count();

    resolve_data_type(table->column_data_type(column_id), [&](const auto column_data_type) {
      using ColumnDataType = typename decltype(column_data_type)::type;

      for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
        const auto& segment = table->get_chunk(chunk_index)->get_segment(column_id);

        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (!position.is_null()) {
            auto seed = size_t{4615968};
            boost::hash_combine(seed, position.value());
            _bloom_filter->insert(static_cast<uint64_t>(seed));
          }
        });
      }
    });
  }

  std::shared_ptr<Table> _create_reduced_table() {
    const auto input_table = left_input_table();
    const auto chunk_count = input_table->chunk_count();
    const auto column_id = _predicate.column_ids.first;

    auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
    output_chunks.reserve(chunk_count);

    resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
      using ColumnDataType = typename decltype(column_data_type)::type;

      for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
        const auto& chunk = input_table->get_chunk(chunk_index);
        const auto& segment = chunk->get_segment(column_id);
        auto matches = std::make_shared<RowIDPosList>();
        matches->guarantee_single_chunk();

        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (!position.is_null()) {
            auto seed = size_t{4615968};
            boost::hash_combine(seed, position.value());

            if (_bloom_filter->probe(static_cast<uint64_t>(seed))) {
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

    return output_table;
  }

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override {}

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override {
        return std::make_shared<Reduce>(copied_left_input, copied_right_input, _predicate);
      }

  const OperatorJoinPredicate _predicate;
  std::shared_ptr<BloomFilter<20, 2>> _bloom_filter;
  std::shared_ptr<MinMaxFilter> _min_max_filter;
};

}  // namespace hyrise