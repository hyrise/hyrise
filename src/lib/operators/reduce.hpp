#pragma once

#include <atomic>
#include <memory>
#include <chrono>
#include <map>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/bloom_filter.hpp"
#include "utils/min_max_predicate.hpp"
#include "utils/timer.hpp"

namespace hyrise {

enum class ReduceMode : uint8_t { Build, Probe, ProbeAndBuild };

enum class UseMinMax : bool { Yes = true, No = false };

enum class ReduceOperatorSteps : uint8_t {
    Iteration,
    OutputWriting,
    FilterMerging
};

template <ReduceMode reduce_mode, UseMinMax use_min_max>
class Reduce : public AbstractReadOnlyOperator {
 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate)
      : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input, std::make_unique<PerformanceData>()}, _predicate{predicate} {}

  const std::string& name() const override {
    static const auto name = std::string{"Reduce"};
    return name;
  }

  std::shared_ptr<BloomFilter<20, 2>> get_bloom_filter() const {
    return _bloom_filter;
  }

  std::shared_ptr<BaseMinMaxPredicate> get_min_max_predicate() const {
    return _min_max_predicate;
  }

  using OperatorSteps = ReduceOperatorSteps;
  struct PerformanceData : public OperatorPerformanceData<OperatorSteps> {
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    // std::cout << "Reducer called with mode: " << magic_enum::enum_name(reduce_mode)
    //           << " use_min_max: " << magic_enum::enum_name(use_min_max) << "\n";
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
      using DataType = typename decltype(column_data_type)::type;

      const auto chunk_count = input_table->chunk_count();
      auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
      output_chunks.resize(chunk_count);

      auto new_bloom_filter = std::shared_ptr<BloomFilter<20, 2>>{};
      std::shared_ptr<MinMaxPredicate<DataType>> new_min_max_predicate;

      if constexpr (reduce_mode != ReduceMode::Probe) {
        new_bloom_filter = std::make_shared<BloomFilter<20, 2>>();

        if constexpr (use_min_max == UseMinMax::Yes) {
          new_min_max_predicate = std::make_shared<MinMaxPredicate<DataType>>();
        }
      }

      if constexpr (reduce_mode != ReduceMode::Build) {
        Assert(_right_input->executed(), "Build Reducer was not executed.");
        const auto build_reduce =
            std::dynamic_pointer_cast<const Reduce<ReduceMode::Build, UseMinMax::No>>(_right_input);
        Assert(build_reduce, "Failed to cast build reduce.");

        _bloom_filter = build_reduce->get_bloom_filter();
        _min_max_predicate = build_reduce->get_min_max_predicate();
      }

      auto minimum = DataType{};
      auto maximum = DataType{};

      if constexpr (reduce_mode != ReduceMode::Build && use_min_max == UseMinMax::Yes) {
        Assert(_min_max_predicate, "Min max filter is null.");
        auto casted_min_max_predicate = std::dynamic_pointer_cast<MinMaxPredicate<DataType>>(_min_max_predicate);
        Assert(casted_min_max_predicate, "Failed to cast min max filter.");
        minimum = casted_min_max_predicate->min_value();
        maximum = casted_min_max_predicate->max_value();
      }

      const auto worker_count = static_cast<uint32_t>(Hyrise::get().topology.num_cpus());
      const auto chunks_per_worker = ChunkID{(static_cast<uint32_t>(chunk_count) + worker_count - 1) / worker_count};

      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(worker_count);

      std::vector<std::chrono::nanoseconds> job_iteration_times(worker_count);
      std::vector<std::chrono::nanoseconds> job_output_times(worker_count);
      std::vector<std::chrono::nanoseconds> job_merge_times(worker_count);

      for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; chunk_index += chunks_per_worker) {

        const auto job = [&, chunk_index]() mutable {
          const auto job_index = static_cast<uint32_t>(chunk_index / chunks_per_worker);

          auto partial_bloom_filter = std::shared_ptr<BloomFilter<20, 2>>{};

          auto partial_minimum = std::numeric_limits<DataType>::max();
          auto partial_maximum = std::numeric_limits<DataType>::lowest();

          if constexpr (reduce_mode != ReduceMode::Probe) {
            partial_bloom_filter = std::make_shared<BloomFilter<20, 2>>();
          }

          auto last_chunk_index = chunk_index + chunks_per_worker;
          if (last_chunk_index > chunk_count) {
            last_chunk_index = chunk_count;
          }

          auto timer = Timer{};
          auto local_scan = std::chrono::nanoseconds{0};
          auto local_output = std::chrono::nanoseconds{0};
          auto local_merge = std::chrono::nanoseconds{0};

          auto hasher = boost::hash<DataType>{};
          for (; chunk_index < last_chunk_index; ++chunk_index) {
            const auto& input_chunk = input_table->get_chunk(chunk_index);
            const auto& input_segment = input_chunk->get_segment(column_id);

            auto matches = std::make_shared<RowIDPosList>();
            matches->reserve(input_chunk->size() / 2);

            timer.lap();

            segment_iterate<DataType>(*input_segment, [&](const auto& position) {
              if (!position.is_null()) {
                auto hash = hasher(position.value());
                // std::cout << "Hash: " << hash << " for value " << position.value() << "\n";

                if constexpr (reduce_mode == ReduceMode::Build) {
                  partial_bloom_filter->insert(static_cast<uint64_t>(hash));

                  if constexpr (use_min_max == UseMinMax::Yes) {
                    partial_minimum = std::min(partial_minimum, position.value());
                    partial_maximum = std::max(partial_maximum, position.value());
                  }
                } else {
                  auto found = _bloom_filter->probe(static_cast<uint64_t>(hash));

                  if constexpr (use_min_max == UseMinMax::Yes && std::is_same_v<DataType, int32_t>) {
                    using UnsignedDataType = std::make_unsigned_t<DataType>;
                    const auto value_difference = static_cast<UnsignedDataType>(maximum - minimum);
                    const auto diff = static_cast<UnsignedDataType>(position.value() - minimum);
                    found &= diff <= value_difference;
                  }

                  if (found) {
                    matches->emplace_back(chunk_index, position.chunk_offset());

                    if constexpr (reduce_mode == ReduceMode::ProbeAndBuild) {
                      partial_bloom_filter->insert(static_cast<uint64_t>(hash));

                      if constexpr (use_min_max == UseMinMax::Yes) {
                        partial_minimum = std::min(partial_minimum, position.value());
                        partial_maximum = std::max(partial_maximum, position.value());
                      }
                    }
                  }
                }
              }
            });

            local_scan += timer.lap();

            if constexpr (reduce_mode != ReduceMode::Build) {
              if (!matches->empty()) {
                const auto column_count = input_table->column_count();
                auto output_segments = Segments{};
                output_segments.reserve(column_count);

                if (input_table->type() == TableType::References) {
                  if (matches->size() == input_chunk->size()) {
                    for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
                      output_segments.emplace_back(input_chunk->get_segment(column_index));
                    }
                  } else {
                    auto filtered_pos_lists =
                        std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>>{};

                    for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
                      auto reference_segment =
                          std::dynamic_pointer_cast<const ReferenceSegment>(input_chunk->get_segment(column_index));
                      DebugAssert(reference_segment, "All segments should be of type ReferenceSegment.");

                      const auto pos_list_in = reference_segment->pos_list();

                      const auto referenced_table = reference_segment->referenced_table();
                      const auto referenced_column_id = reference_segment->referenced_column_id();

                      auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

                      if (!filtered_pos_list) {
                        filtered_pos_list = std::make_shared<RowIDPosList>(matches->size());
                        if (pos_list_in->references_single_chunk()) {
                          filtered_pos_list->guarantee_single_chunk();
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

                  const auto output_pos_list =
                      matches->size() == input_chunk->size()
                          ? static_cast<std::shared_ptr<AbstractPosList>>(
                                std::make_shared<EntireChunkPosList>(chunk_index, input_chunk->size()))
                          : static_cast<std::shared_ptr<AbstractPosList>>(matches);

                  for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
                    const auto ref_segment_out =
                        std::make_shared<ReferenceSegment>(input_table, column_index, output_pos_list);
                    output_segments.push_back(ref_segment_out);
                  }
                }

                const auto output_chunk =
                    std::make_shared<Chunk>(output_segments, nullptr, input_chunk->get_allocator());
                output_chunk->set_immutable();
                if (!input_chunk->individually_sorted_by().empty()) {
                  output_chunk->set_individually_sorted_by(input_chunk->individually_sorted_by());
                }
                output_chunks[chunk_index] = output_chunk;

              }

              local_output += timer.lap();
            }
          } 

          // Measure and store merge cost separately
          if constexpr (reduce_mode != ReduceMode::Probe) {
            new_bloom_filter->merge_from(*partial_bloom_filter);
            if constexpr (use_min_max == UseMinMax::Yes) {
              new_min_max_predicate->merge_from(partial_minimum, partial_maximum);
            }

            local_merge += timer.lap();
          }

          job_iteration_times[job_index] = local_scan;
          job_output_times[job_index] = local_output;
          job_merge_times[job_index] = local_merge;
        };

        jobs.emplace_back(std::make_shared<JobTask>(job));
      }

      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

      if constexpr (reduce_mode != ReduceMode::Build) {
        std::erase_if(output_chunks, [](const auto& ptr) {
          return ptr == nullptr;
        });
        output_table = std::make_shared<const Table>(input_table->column_definitions(), TableType::References,
                                                     std::move(output_chunks));
      }

      if constexpr (reduce_mode != ReduceMode::Probe) {
        _bloom_filter = new_bloom_filter;
        _min_max_predicate = new_min_max_predicate;
      }

      auto& reduce_performance_data = static_cast<PerformanceData&>(*performance_data);  // fixed pointer usage
      const auto sum_time = [](const std::vector<std::chrono::nanoseconds>& v) {
        std::chrono::nanoseconds s{0};
        for (const auto& x : v) s += x;
        return s;
      };

      const auto total_scan = sum_time(job_iteration_times);
      const auto total_output = sum_time(job_output_times);
      const auto total_merge = sum_time(job_merge_times);

      reduce_performance_data.set_step_runtime(OperatorSteps::Iteration, total_scan);
      reduce_performance_data.set_step_runtime(OperatorSteps::OutputWriting, total_output);
      reduce_performance_data.set_step_runtime(OperatorSteps::FilterMerging, total_merge);
    });

    // std::cout << "About to exit reducer\n";
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
  std::shared_ptr<BaseMinMaxPredicate> _min_max_predicate;
};

}  // namespace hyrise