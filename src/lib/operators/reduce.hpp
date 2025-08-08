#pragma once

#include <atomic>
#include <memory>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/bloom_filter.hpp"
#include "utils/min_max_filter.hpp"

namespace hyrise {

enum class ReduceMode : uint8_t {
  Build,
  Probe,
  BuildAndProbe
};

enum class UseMinMax : bool {
  Yes = true,
  No = false
};

template <ReduceMode reduce_mode, UseMinMax use_min_max>
class Reduce : public AbstractReadOnlyOperator {
 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate)
      : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input}, _predicate{predicate} {}

  const std::string& name() const override {
    static const auto name = std::string{"Reduce"};
    return name;
  }

  std::shared_ptr<BloomFilter<20,2>> get_bloom_filter() const {
    return _bloom_filter;
  }

  std::shared_ptr<BaseMinMaxFilter> get_min_max_filter() const {
    return _min_max_filter;
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    // std::cout << "Reducer called.\n";
    std::shared_ptr<const Table> input_table;
    std::shared_ptr<const Table> output_table;
    auto column_id = ColumnID{};

    if constexpr (reduce_mode == ReduceMode::Build) {
      input_table = right_input_table();
      output_table = input_table;
      column_id = _predicate.column_ids.second;
    } else {
      input_table = left_input_table();
      column_id = _predicate.column_ids.first;
    }

    resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
      using ColumnDataType = typename decltype(column_data_type)::type;

      const auto chunk_count = input_table->chunk_count();
      auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
      output_chunks.reserve(chunk_count);

      // If reduce_mode is ReduceMode::BuildAndProbe, new filters must be build while the old ones are probed.
      std::shared_ptr<BloomFilter<20, 2>> new_bloom_filter;
      std::shared_ptr<MinMaxFilter<ColumnDataType>> new_min_max_filter;

      if constexpr (reduce_mode == ReduceMode::Build) {
        _bloom_filter = std::make_shared<BloomFilter<20, 2>>();
        _min_max_filter = std::make_shared<MinMaxFilter<ColumnDataType>>();
      } else {
        Assert(_right_input->executed(), "Build Reducer was not executed.");
        const auto build_reduce = std::dynamic_pointer_cast<const Reduce<ReduceMode::Build, UseMinMax::Yes>>(_right_input);
        Assert(build_reduce, "Failed to cast probe reduce.");

        _bloom_filter = build_reduce->get_bloom_filter();
        _min_max_filter = build_reduce->get_min_max_filter();

        if constexpr (reduce_mode == ReduceMode::BuildAndProbe) {
          new_bloom_filter = std::make_shared<BloomFilter<20, 2>>();
          new_min_max_filter = std::make_shared<MinMaxFilter<ColumnDataType>>();
        }
      }

      for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
        const auto& input_chunk = input_table->get_chunk(chunk_index);
        const auto& input_segment = input_chunk->get_segment(column_id);

        auto casted_min_max_filter = std::shared_ptr<MinMaxFilter<ColumnDataType>>();
        if constexpr (use_min_max == UseMinMax::Yes) {
          casted_min_max_filter = std::dynamic_pointer_cast<MinMaxFilter<ColumnDataType>>(_min_max_filter);
        }

        auto matches = std::make_shared<RowIDPosList>();
        if constexpr (reduce_mode != ReduceMode::Build) {
          matches->guarantee_single_chunk();
        }

        segment_iterate<ColumnDataType>(*input_segment, [&](const auto& position) {
          if (!position.is_null()) {
            auto seed = size_t{4615968};
            boost::hash_combine(seed, position.value());

            if constexpr (reduce_mode == ReduceMode::Build) {
              _bloom_filter->insert(static_cast<uint64_t>(seed));

              if constexpr (use_min_max == UseMinMax::Yes) {
                casted_min_max_filter->insert(position.value());
              }
            } else {
              bool found = _bloom_filter->probe(static_cast<uint64_t>(seed));

              if constexpr (use_min_max == UseMinMax::Yes) {
                found = casted_min_max_filter->probe(position.value());
              }

              if (found) {
                matches->emplace_back(RowID{chunk_index, position.chunk_offset()});

                if constexpr (reduce_mode == ReduceMode::BuildAndProbe) {
                  new_bloom_filter->insert(static_cast<uint64_t>(seed));

                  if constexpr (use_min_max == UseMinMax::Yes) {
                    new_min_max_filter->insert(position.value());
                  }
                }
              }
            }
          }
        });

        if constexpr (reduce_mode != ReduceMode::Build) {

          if (!matches->empty()) {
            const auto column_count = input_table->column_count();
            auto output_segments = Segments{};
            output_segments.reserve(column_count);

            auto keep_chunk_sort_order = true;
            if (input_table->type() == TableType::References) {
              if (matches->size() == input_chunk->size()) {
                for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
                  output_segments.emplace_back(input_chunk->get_segment(column_index));
                }
              } else {
                auto filtered_pos_lists =
                    std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>>{};

                for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
                  auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(input_chunk->get_segment(column_index));
                  DebugAssert(reference_segment, "All segments should be of type ReferenceSegment.");

                  const auto pos_list_in = reference_segment->pos_list();

                  const auto referenced_table = reference_segment->referenced_table();
                  const auto referenced_column_id = reference_segment->referenced_column_id();

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
                      std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, filtered_pos_list);
                  output_segments.push_back(ref_segment_out);
                }
              }
            } else {
              matches->guarantee_single_chunk();

              const auto output_pos_list = matches->size() == input_chunk->size()
                                               ? static_cast<std::shared_ptr<AbstractPosList>>(
                                                     std::make_shared<EntireChunkPosList>(chunk_index, input_chunk->size()))
                                               : static_cast<std::shared_ptr<AbstractPosList>>(matches);

              for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
                const auto ref_segment_out = std::make_shared<ReferenceSegment>(input_table, column_index, output_pos_list);
                output_segments.push_back(ref_segment_out);
              }
            }

            const auto out_chunk = std::make_shared<Chunk>(output_segments, nullptr, input_chunk->get_allocator());
            out_chunk->set_immutable();
            if (keep_chunk_sort_order && !input_chunk->individually_sorted_by().empty()) {
              out_chunk->set_individually_sorted_by(input_chunk->individually_sorted_by());
            }
            output_chunks.emplace_back(out_chunk);
          }

        }
      }

      output_table = std::make_shared<const Table>(input_table->column_definitions(), TableType::References, std::move(output_chunks));

      if constexpr (reduce_mode == ReduceMode::BuildAndProbe) {
        _bloom_filter = new_bloom_filter;
        _min_max_filter = new_min_max_filter;
      }
    });

    return output_table;
  }

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override {}

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override {
    return std::make_shared<Reduce<reduce_mode, use_min_max>>(copied_left_input, copied_right_input, _predicate);
  }

  const OperatorJoinPredicate _predicate;
  std::shared_ptr<BloomFilter<20, 2>> _bloom_filter;
  std::shared_ptr<BaseMinMaxFilter> _min_max_filter;
};

}  // namespace hyrise