#include "reduce.hpp"

#include <memory>

#include <boost/math/distributions/poisson.hpp>

#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/timer.hpp"

namespace hyrise {

inline std::shared_ptr<BaseBloomFilter> make_bloom_filter(const uint8_t filter_size_exponent,
                                                          const uint8_t block_size_exponent, const uint8_t k) {
  switch (block_size_exponent) {
    case 0: {
      switch (filter_size_exponent) {
        case 18:
          switch (k) {
            case 1:
              return std::make_shared<BloomFilter<18, 1>>();
            case 2:
              return std::make_shared<BloomFilter<18, 2>>();
            default:
              break;
          }
          break;
        case 19:
          switch (k) {
            case 1:
              return std::make_shared<BloomFilter<19, 1>>();
            case 2:
              return std::make_shared<BloomFilter<19, 2>>();
            default:
              break;
          }
          break;
        case 20:
          switch (k) {
            case 1:
              return std::make_shared<BloomFilter<20, 1>>();
            case 2:
              return std::make_shared<BloomFilter<20, 2>>();
            default:
              break;
          }
          break;
        case 21:
          switch (k) {
            case 1:
              return std::make_shared<BloomFilter<21, 1>>();
            case 2:
              return std::make_shared<BloomFilter<21, 2>>();
            default:
              break;
          }
          break;
        case 23:
          switch (k) {
            case 1:
              return std::make_shared<BloomFilter<23, 1>>();
            case 2:
              return std::make_shared<BloomFilter<23, 2>>();
            default:
              break;
          }
          break;
        default:
          break;
      }
      break;
    }
    case 8: {
      switch (filter_size_exponent) {
        case 18:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<18, 8, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<18, 8, 2>>();
            default:
              break;
          }
          break;
        case 19:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<19, 8, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<19, 8, 2>>();
            default:
              break;
          }
          break;
        case 20:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<20, 8, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<20, 8, 2>>();
            default:
              break;
          }
          break;
        case 21:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<21, 8, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<21, 8, 2>>();
            default:
              break;
          }
          break;
        case 23:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<23, 8, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<23, 8, 2>>();
            default:
              break;
          }
          break;
        default:
          break;
      }
      break;
    }
    case 9: {
      switch (filter_size_exponent) {
        case 18:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<18, 9, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<18, 9, 2>>();
            case 3:
              return std::make_shared<BlockBloomFilter<18, 9, 3>>();
            default:
              break;
          }
          break;
        case 19:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<19, 9, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<19, 9, 2>>();
            case 3:
              return std::make_shared<BlockBloomFilter<19, 9, 3>>();
            default:
              break;
          }
          break;
        case 20:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<20, 9, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<20, 9, 2>>();
            case 3:
              return std::make_shared<BlockBloomFilter<20, 9, 3>>();
            default:
              break;
          }
          break;
        case 21:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<21, 9, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<21, 9, 2>>();
            case 3:
              return std::make_shared<BlockBloomFilter<21, 9, 3>>();
            default:
              break;
          }
          break;
        case 23:
          switch (k) {
            case 1:
              return std::make_shared<BlockBloomFilter<23, 9, 1>>();
            case 2:
              return std::make_shared<BlockBloomFilter<23, 9, 2>>();
            case 3:
              return std::make_shared<BlockBloomFilter<23, 9, 3>>();
            default:
              break;
          }
          break;
        default:
          break;
      }
      break;
    }
    default:
      break;
  }

  Fail("Unsupported bloom filter parameter combination.");
}

static double false_positive_rate(double m,
    double n,
    double k) {
  return std::pow(1.0 - std::pow(1.0 - (1.0 / m), k * n), k);
}

double false_positive_rate_blocked(double filter_size_bits,
            double n,
            double k,
            double B = 512, /* block size in bits */
            double epsilon = 0.000001) {
  double f = 0;
  double c = filter_size_bits / n;
  double lambda = B / c;
  boost::math::poisson_distribution<> poisson(lambda);

  double k_act = k;

  double d_sum = 0.0;
  double i = 0;
  while ((d_sum + epsilon) < 1.0) {
    auto d = boost::math::pdf(poisson, i);
    d_sum += d;
    f += d * false_positive_rate(B, i, k_act);
    i++;
  }
  return f;
}

Reduce::Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
               const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate,
               const ReduceMode reduce_mode, const UseMinMax use_min_max, uint8_t filter_size_exponent,
               uint8_t block_size_exponent, uint8_t k)
    : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input, std::make_unique<PerformanceData>()},
      _predicate{predicate},
      _reduce_mode{reduce_mode},
      _use_min_max{use_min_max},
      _filter_size_exponent{filter_size_exponent},
      _block_size_exponent{block_size_exponent},
      _k{k} {}

const std::string& Reduce::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

std::shared_ptr<BaseBloomFilter> Reduce::get_bloom_filter() const {
  return _bloom_filter;
}

std::shared_ptr<BaseMinMaxPredicate> Reduce::get_min_max_predicate() const {
  return _min_max_predicate;
}

std::shared_ptr<const Table> Reduce::_on_execute() {
  switch (_reduce_mode) {
    case ReduceMode::Build:
      switch (_use_min_max) {
        case UseMinMax::Yes:
          return _execute_build<UseMinMax::Yes>();
        case UseMinMax::No:
          return _execute_build<UseMinMax::No>();
      }
      break;

    case ReduceMode::Probe:
      switch (_use_min_max) {
        case UseMinMax::Yes:
          return _execute_probe<UseMinMax::Yes>();
        case UseMinMax::No:
          return _execute_probe<UseMinMax::No>();
      }
      break;

    case ReduceMode::ProbeAndBuild:
      switch (_use_min_max) {
        case UseMinMax::Yes:
          return _execute_probe_and_build<UseMinMax::Yes>();
        case UseMinMax::No:
          return _execute_probe_and_build<UseMinMax::No>();
      }
      break;
  }
  Fail("Invalid ReduceMode / UseMinMax combination");
}

template <UseMinMax use_min_max>
std::shared_ptr<const Table> Reduce::_execute_build() {
  std::shared_ptr<const Table> input_table = right_input_table();
  auto column_id = _predicate.column_ids.second;

  if (_filter_size_exponent == 0) {
    const auto input_row_count = input_table->row_count();
    std::cout << "Reduce input row count: " << input_row_count << std::endl;

    const auto max_k = uint8_t{3};
    const auto max_false_positive_rate = double{0.05};

    const auto l1_size = uint32_t{SYSTEM_L1_CACHE_SIZE};
    const auto l2_size = uint32_t{SYSTEM_L2_CACHE_SIZE};
    const auto l1_size_bits = l1_size * 8;
    const auto l2_size_bits = l2_size * 8;
    Assert(l1_size > 0 && l2_size > 0, "Something went wrong during cache calculation.");

    const auto l1_exponent = static_cast<uint8_t>(std::bit_width(l1_size_bits) - 1);
    const auto l2_exponent = static_cast<uint8_t>(std::bit_width(l2_size_bits) - 1);
    std::cout << "System L1 size: " << l1_size << ", bits: " << l1_size_bits << ", exponent: " << static_cast<int>(l1_exponent) << std::endl;
    std::cout << "System L2 size: " << l2_size << ", bits: " << l2_size_bits << ", exponent: " << static_cast<int>(l2_exponent) << std::endl;

    _filter_size_exponent = l1_exponent;


    auto false_positive_rates = std::vector<std::pair<uint8_t, double>>{};

    for (auto k_index = uint8_t{1}; k_index <= max_k; ++k_index) {
      false_positive_rates.emplace_back(k_index, false_positive_rate_blocked(l1_size, static_cast<double>(input_row_count), k_index));
      std::cout << "filter_size_exponent: " << static_cast<int>(l1_exponent) << ", k: " << static_cast<int>(k_index) << ", fpr: " << false_positive_rates.back().second  << std::endl;
    }

    auto [min_k, min_false_positive_rate] = *std::min_element(false_positive_rates.begin(), false_positive_rates.end(), [](auto& a, auto& b) {return a.second < b.second;});

    if (min_false_positive_rate > max_false_positive_rate) {
      false_positive_rates.clear();

      for (auto k_index = uint8_t{1}; k_index <= max_k; ++k_index) {
        false_positive_rates.emplace_back(k_index, false_positive_rate_blocked(l2_size, static_cast<double>(input_row_count), k_index));
        std::cout << "filter_size_exponent: " << static_cast<int>(l2_exponent) << ", k: " << static_cast<int>(k_index) << ", fpr: " << false_positive_rates.back().second  << std::endl;
      }

      std::tie(min_k, min_false_positive_rate) = *std::min_element(false_positive_rates.begin(), false_positive_rates.end(), [](auto& a, auto& b) {return a.second < b.second;});
    }

    _k = min_k;
    std::cout << "Selected exponent: " << static_cast<int>(_filter_size_exponent) << ", k: " << static_cast<int>(_k) << std::endl;
  }

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using DataType = typename decltype(column_data_type)::type;

    auto new_bloom_filter = make_bloom_filter(_filter_size_exponent, _block_size_exponent, _k);
    std::shared_ptr<MinMaxPredicate<DataType>> new_min_max_predicate;

    if constexpr (use_min_max == UseMinMax::Yes) {
      new_min_max_predicate = std::make_shared<MinMaxPredicate<DataType>>();
    }

    const auto worker_count = uint32_t{1};  //static_cast<uint32_t>(Hyrise::get().topology.num_cpus());
    // std::cout << "Worker count: " << worker_count << "\n";
    const auto chunk_count = input_table->chunk_count();
    const auto chunks_per_worker = ChunkID{(static_cast<uint32_t>(chunk_count) + worker_count - 1) / worker_count};

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(worker_count);

    std::atomic<size_t> total_iteration_time{0};
    std::atomic<size_t> total_output_time{0};
    std::atomic<size_t> total_merge_time{0};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; chunk_index += chunks_per_worker) {
      const auto job = [&, chunk_index]() mutable {
        auto partial_bloom_filter = make_bloom_filter(_filter_size_exponent, _block_size_exponent, _k);

        auto partial_minimum = std::numeric_limits<DataType>::max();
        auto partial_maximum = std::numeric_limits<DataType>::lowest();

        auto last_chunk_index = chunk_index + chunks_per_worker;
        if (last_chunk_index > chunk_count) {
          last_chunk_index = chunk_count;
        }

        auto timer = Timer{};
        auto local_scan = std::chrono::nanoseconds{0};
        auto local_output = std::chrono::nanoseconds{0};
        auto local_merge = std::chrono::nanoseconds{0};

        resolve_bloom_filter_type(*partial_bloom_filter, [&](auto& resolved_partial_bloom_filter) {
          // std::cout << "Resolved partial bloom filter.\n";

          for (; chunk_index < last_chunk_index; ++chunk_index) {
            const auto& input_chunk = input_table->get_chunk(chunk_index);
            const auto& input_segment = input_chunk->get_segment(column_id);

            timer.lap();

            segment_iterate<DataType>(*input_segment, [&](const auto& position) {
              if (!position.is_null()) {
                auto hash = size_t{11400714819323198485ul};
                boost::hash_combine(hash, position.value());
                // std::cout << "Hash: " << hash << " for value " << position.value() << "\n";

                resolved_partial_bloom_filter.insert(static_cast<uint64_t>(hash));

                if constexpr (use_min_max == UseMinMax::Yes) {
                  partial_minimum = std::min(partial_minimum, position.value());
                  partial_maximum = std::max(partial_maximum, position.value());
                }
              }
            });

            local_scan += timer.lap();
          }
        });

        new_bloom_filter->merge_from(*partial_bloom_filter);
        if constexpr (use_min_max == UseMinMax::Yes) {
          new_min_max_predicate->merge_from(partial_minimum, partial_maximum);
        }

        local_merge += timer.lap();

        total_iteration_time.fetch_add(local_scan.count(), std::memory_order_relaxed);
        total_output_time.fetch_add(local_output.count(), std::memory_order_relaxed);
        total_merge_time.fetch_add(local_merge.count(), std::memory_order_relaxed);
      };

      jobs.emplace_back(std::make_shared<JobTask>(job));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    _bloom_filter = new_bloom_filter;
    _min_max_predicate = new_min_max_predicate;

    auto& reduce_performance_data = static_cast<PerformanceData&>(*performance_data);

    reduce_performance_data.set_step_runtime(OperatorSteps::Iteration,
                                             std::chrono::nanoseconds{total_iteration_time.load()});
    reduce_performance_data.set_step_runtime(OperatorSteps::OutputWriting,
                                             std::chrono::nanoseconds{total_output_time.load()});
    reduce_performance_data.set_step_runtime(OperatorSteps::FilterMerging,
                                             std::chrono::nanoseconds{total_merge_time.load()});
  });

  return input_table;
}

template <UseMinMax use_min_max>
std::shared_ptr<const Table> Reduce::_execute_probe() {
  std::shared_ptr<const Table> input_table = left_input_table();
  std::shared_ptr<const Table> output_table;
  auto column_id = _predicate.column_ids.first;

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using DataType = typename decltype(column_data_type)::type;

    const auto chunk_count = input_table->chunk_count();
    auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
    output_chunks.resize(chunk_count);

    Assert(_right_input->executed(), "Build Reducer was not executed.");
    const auto build_reduce = std::dynamic_pointer_cast<const Reduce>(_right_input);
    Assert(build_reduce, "Failed to cast build reduce.");

    _bloom_filter = build_reduce->get_bloom_filter();
    _min_max_predicate = build_reduce->get_min_max_predicate();

    auto minimum = DataType{};
    auto maximum = DataType{};

    using UnsignedDataType = std::conditional_t<std::is_same_v<DataType, int32_t>, uint32_t, DataType>;
    auto value_difference = UnsignedDataType{};
    if constexpr (use_min_max == UseMinMax::Yes && std::is_same_v<DataType, int32_t>) {
      Assert(_min_max_predicate, "Min max filter is null.");
      auto casted_min_max_predicate = std::dynamic_pointer_cast<MinMaxPredicate<DataType>>(_min_max_predicate);
      Assert(casted_min_max_predicate, "Failed to cast min max filter.");
      minimum = casted_min_max_predicate->min_value();
      maximum = casted_min_max_predicate->max_value();
      value_difference = static_cast<UnsignedDataType>(maximum - minimum);
      // TODO: Assert that diff is not too large?
    }

    const auto worker_count = uint32_t{1};
    // const auto worker_count = static_cast<uint32_t>(Hyrise::get().topology.num_cpus());
    // std::cout << "Worker count: " << worker_count << "\n";
    const auto chunks_per_worker = ChunkID{(static_cast<uint32_t>(chunk_count) + worker_count - 1) / worker_count};

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(worker_count);

    std::atomic<size_t> total_iteration_time{0};
    std::atomic<size_t> total_output_time{0};
    std::atomic<size_t> total_merge_time{0};

    resolve_bloom_filter_type(*_bloom_filter, [&](auto& resolved_bloom_filter) {
      // std::cout << "Resolved global bloom filter.\n";

      for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; chunk_index += chunks_per_worker) {
        const auto job = [&, chunk_index]() mutable {
          auto last_chunk_index = chunk_index + chunks_per_worker;
          if (last_chunk_index > chunk_count) {
            last_chunk_index = chunk_count;
          }

          auto timer = Timer{};
          auto local_scan = std::chrono::nanoseconds{0};
          auto local_output = std::chrono::nanoseconds{0};
          auto local_merge = std::chrono::nanoseconds{0};

          for (; chunk_index < last_chunk_index; ++chunk_index) {
            const auto& input_chunk = input_table->get_chunk(chunk_index);
            const auto& input_segment = input_chunk->get_segment(column_id);

            auto matches = std::make_shared<RowIDPosList>();
            matches->reserve(input_chunk->size() / 2);

            timer.lap();

            segment_iterate<DataType>(*input_segment, [&](const auto& position) {
              if (!position.is_null()) {
                auto hash = size_t{11400714819323198485ul};
                boost::hash_combine(hash, position.value());
                // std::cout << "Hash: " << hash << " for value " << position.value() << "\n";

                auto found = resolved_bloom_filter.probe(static_cast<uint64_t>(hash));

                if constexpr (use_min_max == UseMinMax::Yes && std::is_same_v<DataType, int32_t>) {
                  const auto diff = static_cast<UnsignedDataType>(position.value() - minimum);
                  found &= diff <= value_difference;
                }

                if (found) {
                  matches->emplace_back(chunk_index, position.chunk_offset());
                }
              }
            });

            local_scan += timer.lap();

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

              const auto output_chunk = std::make_shared<Chunk>(output_segments, nullptr, input_chunk->get_allocator());
              output_chunk->set_immutable();
              if (!input_chunk->individually_sorted_by().empty()) {
                output_chunk->set_individually_sorted_by(input_chunk->individually_sorted_by());
              }
              output_chunks[chunk_index] = output_chunk;
            }

            local_output += timer.lap();
          }

          total_iteration_time.fetch_add(local_scan.count(), std::memory_order_relaxed);
          total_output_time.fetch_add(local_output.count(), std::memory_order_relaxed);
          total_merge_time.fetch_add(local_merge.count(), std::memory_order_relaxed);
        };

        jobs.emplace_back(std::make_shared<JobTask>(job));
      }
    });

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    std::erase_if(output_chunks, [](const auto& ptr) {
      return ptr == nullptr;
    });
    output_table = std::make_shared<const Table>(input_table->column_definitions(), TableType::References,
                                                 std::move(output_chunks));

    auto& reduce_performance_data = static_cast<PerformanceData&>(*performance_data);
    reduce_performance_data.set_step_runtime(OperatorSteps::Iteration,
                                             std::chrono::nanoseconds{total_iteration_time.load()});
    reduce_performance_data.set_step_runtime(OperatorSteps::OutputWriting,
                                             std::chrono::nanoseconds{total_output_time.load()});
    reduce_performance_data.set_step_runtime(OperatorSteps::FilterMerging,
                                             std::chrono::nanoseconds{total_merge_time.load()});
  });

  return output_table;
}

template <UseMinMax use_min_max>
std::shared_ptr<const Table> Reduce::_execute_probe_and_build() {
  std::shared_ptr<const Table> input_table = left_input_table();
  std::shared_ptr<const Table> output_table;
  auto column_id = _predicate.column_ids.first;

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using DataType = typename decltype(column_data_type)::type;

    const auto chunk_count = input_table->chunk_count();
    auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
    output_chunks.resize(chunk_count);

    auto new_bloom_filter = std::shared_ptr<BaseBloomFilter>{};
    std::shared_ptr<MinMaxPredicate<DataType>> new_min_max_predicate;

    new_bloom_filter = make_bloom_filter(_filter_size_exponent, _block_size_exponent, _k);

    if constexpr (use_min_max == UseMinMax::Yes) {
      new_min_max_predicate = std::make_shared<MinMaxPredicate<DataType>>();
    }

    Assert(_right_input->executed(), "Build Reducer was not executed.");
    const auto build_reduce = std::dynamic_pointer_cast<const Reduce>(_right_input);
    Assert(build_reduce, "Failed to cast build reduce.");

    _bloom_filter = build_reduce->get_bloom_filter();
    _min_max_predicate = build_reduce->get_min_max_predicate();

    auto minimum = DataType{};
    auto maximum = DataType{};

    using UnsignedDataType = std::conditional_t<std::is_same_v<DataType, int32_t>, uint32_t, DataType>;
    auto value_difference = UnsignedDataType{};
    if constexpr (use_min_max == UseMinMax::Yes && std::is_same_v<DataType, int32_t>) {
      Assert(_min_max_predicate, "Min max filter is null.");
      auto casted_min_max_predicate = std::dynamic_pointer_cast<MinMaxPredicate<DataType>>(_min_max_predicate);
      Assert(casted_min_max_predicate, "Failed to cast min max filter.");
      minimum = casted_min_max_predicate->min_value();
      maximum = casted_min_max_predicate->max_value();
    }

    const auto worker_count = uint32_t{1};  //static_cast<uint32_t>(Hyrise::get().topology.num_cpus());
    // const auto worker_count = static_cast<uint32_t>(Hyrise::get().topology.num_cpus());
    // std::cout << "Worker count: " << worker_count << "\n";
    const auto chunks_per_worker = ChunkID{(static_cast<uint32_t>(chunk_count) + worker_count - 1) / worker_count};

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(worker_count);

    std::atomic<size_t> total_iteration_time{0};
    std::atomic<size_t> total_output_time{0};
    std::atomic<size_t> total_merge_time{0};

    resolve_bloom_filter_type(*_bloom_filter, [&](auto& resolved_bloom_filter) {
      using BloomFilterType = std::remove_reference_t<decltype(resolved_bloom_filter)>;
      // std::cout << "Resolved global bloom filter.\n";

      for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; chunk_index += chunks_per_worker) {
        const auto job = [&, chunk_index]() mutable {
          auto partial_bloom_filter = make_bloom_filter(_filter_size_exponent, _block_size_exponent, _k);

          auto partial_minimum = std::numeric_limits<DataType>::max();
          auto partial_maximum = std::numeric_limits<DataType>::lowest();

          auto last_chunk_index = chunk_index + chunks_per_worker;
          if (last_chunk_index > chunk_count) {
            last_chunk_index = chunk_count;
          }

          auto timer = Timer{};
          auto local_scan = std::chrono::nanoseconds{0};
          auto local_output = std::chrono::nanoseconds{0};
          auto local_merge = std::chrono::nanoseconds{0};

          // resolve_bloom_filter_type(*partial_bloom_filter, [&](auto& resolved_partial_bloom_filter) {
          auto& resolved_partial_bloom_filter = static_cast<BloomFilterType&>(*partial_bloom_filter);
          // std::cout << "Resolved partial bloom filter.\n";

          for (; chunk_index < last_chunk_index; ++chunk_index) {
            const auto& input_chunk = input_table->get_chunk(chunk_index);
            const auto& input_segment = input_chunk->get_segment(column_id);

            auto matches = std::make_shared<RowIDPosList>();
            matches->reserve(input_chunk->size() / 2);

            timer.lap();

            segment_iterate<DataType>(*input_segment, [&](const auto& position) {
              if (!position.is_null()) {
                auto hash = size_t{11400714819323198485ul};
                boost::hash_combine(hash, position.value());
                // std::cout << "Hash: " << hash << " for value " << position.value() << "\n";

                auto found = resolved_bloom_filter.probe(static_cast<uint64_t>(hash));

                if constexpr (use_min_max == UseMinMax::Yes && std::is_same_v<DataType, int32_t>) {
                  const auto diff = static_cast<UnsignedDataType>(position.value() - minimum);
                  found &= diff <= value_difference;
                }

                if (found) {
                  matches->emplace_back(chunk_index, position.chunk_offset());

                  resolved_partial_bloom_filter.insert(static_cast<uint64_t>(hash));

                  if constexpr (use_min_max == UseMinMax::Yes) {
                    partial_minimum = std::min(partial_minimum, position.value());
                    partial_maximum = std::max(partial_maximum, position.value());
                  }
                }
              }
            });

            local_scan += timer.lap();

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

              const auto output_chunk = std::make_shared<Chunk>(output_segments, nullptr, input_chunk->get_allocator());
              output_chunk->set_immutable();
              if (!input_chunk->individually_sorted_by().empty()) {
                output_chunk->set_individually_sorted_by(input_chunk->individually_sorted_by());
              }
              output_chunks[chunk_index] = output_chunk;
            }

            local_output += timer.lap();
          }
          // });

          new_bloom_filter->merge_from(*partial_bloom_filter);
          if constexpr (use_min_max == UseMinMax::Yes) {
            new_min_max_predicate->merge_from(partial_minimum, partial_maximum);
          }

          local_merge += timer.lap();

          total_iteration_time.fetch_add(local_scan.count(), std::memory_order_relaxed);
          total_output_time.fetch_add(local_output.count(), std::memory_order_relaxed);
          total_merge_time.fetch_add(local_merge.count(), std::memory_order_relaxed);
        };

        jobs.emplace_back(std::make_shared<JobTask>(job));
      }
    });

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    std::erase_if(output_chunks, [](const auto& ptr) {
      return ptr == nullptr;
    });
    output_table = std::make_shared<const Table>(input_table->column_definitions(), TableType::References,
                                                 std::move(output_chunks));

    _bloom_filter = new_bloom_filter;
    _min_max_predicate = new_min_max_predicate;

    auto& reduce_performance_data = static_cast<PerformanceData&>(*performance_data);

    reduce_performance_data.set_step_runtime(OperatorSteps::Iteration,
                                             std::chrono::nanoseconds{total_iteration_time.load()});
    reduce_performance_data.set_step_runtime(OperatorSteps::OutputWriting,
                                             std::chrono::nanoseconds{total_output_time.load()});
    reduce_performance_data.set_step_runtime(OperatorSteps::FilterMerging,
                                             std::chrono::nanoseconds{total_merge_time.load()});
  });

  // std::cout << "About to exit reducer\n";
  return output_table;
}

void Reduce::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractOperator> Reduce::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Reduce>(copied_left_input, copied_right_input, _predicate, _reduce_mode, _use_min_max);
}

}  // namespace hyrise
