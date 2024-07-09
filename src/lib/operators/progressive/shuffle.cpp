#include "shuffle.hpp"

#include <cmath>
#include <memory>
#include <thread>
#include <span>
#include <vector>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

constexpr auto MAX_SHUFFLE_COLUMNS = uint8_t{8};
}  // namespace

namespace hyrise {

Shuffle::Shuffle(const std::shared_ptr<const AbstractOperator>& input_operator, std::vector<ColumnID>&& column_ids,
                 std::vector<uint8_t>&& partition_counts)
    : AbstractReadOnlyOperator(OperatorType::Shuffle, input_operator, nullptr),
      _column_ids{std::move(column_ids)},
      _partition_counts{std::move(partition_counts)} {
  Assert(_column_ids.size() == _partition_counts.size(), "Unexpected shuffle configuration.");
  Assert(_column_ids.size() <= MAX_SHUFFLE_COLUMNS, "We only support up to " + std::to_string(MAX_SHUFFLE_COLUMNS) + 
                                                    " columns for shuffling.");

  for (const auto& partition_count : _partition_counts) {
    Assert(std::log2(partition_count) == std::ceil(std::log2(partition_count)), "Partition sizes must be a power of two.");
  }
}

const std::string& Shuffle::name() const {
  static const auto name = std::string{"Shuffle"};
  return name;
}

std::shared_ptr<AbstractOperator> Shuffle::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  auto copy_column_ids = _column_ids;
  auto copy_partition_counts = _partition_counts;
  auto copy = std::make_shared<Shuffle>(copied_left_input, std::move(copy_column_ids), std::move(copy_partition_counts));
  return copy;
}

void Shuffle::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Shuffle::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();
  // const auto input_table_row_count = input_table->row_count();
  const auto input_table_chunk_count = input_table->chunk_count();
  const auto partition_column_count = _column_ids.size();
  Assert(input_table->type() == TableType::Data, "Shuffle cannot be executed on reference tables.");

  // Print::print(input_table);

  /**
   * We store a large matrix-like structure that stores for each chunk per input row:
   *   - two fields for ChunkID and ChunkOffset (i.e., the RowID)
   *   - one field (int32_t) per partition column to store the radix value per column value
   *   - one field for the combined radix value
   */
  const auto row_ids_and_radix_values_width = 3 + partition_column_count;
  auto row_ids_and_radix_values = std::vector<std::vector<uint32_t>>(input_table_chunk_count);

  /**
   * Phase #1: determining radix values for all partition columns.
   */
    // For partition column x, of partition colummns xyz, we multiply the value in x by |y|*|z|. For y, we multiply it by
  // |z|.
  auto column_factors = std::array<int32_t, MAX_SHUFFLE_COLUMNS>{};
  for (auto column_offset = size_t{0}; column_offset < partition_column_count; ++column_offset) {
    column_factors[column_offset] = 1;
    for (auto column_factor_offset = size_t{column_offset + 1}; column_factor_offset < partition_column_count; ++column_factor_offset) {
      column_factors[column_offset] *= _partition_counts[column_factor_offset];
    }
  }
  for (auto column_offset = static_cast<int8_t>(partition_column_count); column_offset < MAX_SHUFFLE_COLUMNS; ++column_offset) {
    column_factors[column_offset] = -1;
  }
  
  // for (auto cf : column_factors) { std::cerr << cf << " & "; }
  // std::cerr << "\n";

  auto overall_partition_count = size_t{1};
  for (const auto partition_count : _partition_counts) {
    overall_partition_count *= partition_count;
  }

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(input_table_chunk_count);
  for (auto column_id_offset = size_t{0}; column_id_offset < partition_column_count; ++column_id_offset) {
    const auto column_id = _column_ids[column_id_offset];
    for (auto chunk_id = ChunkID{0}; chunk_id < input_table_chunk_count; ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, column_id_offset, column_id, chunk_id] () {
        // auto histogram_subspan = std::span(histogram).subspan(chunk_id * partition_count, partition_count);

        const auto& chunk = input_table->get_chunk(chunk_id);
        if (!chunk) {
          return;
        }

        row_ids_and_radix_values[chunk_id].resize(chunk->size() * row_ids_and_radix_values_width);
        const auto& segment = chunk->get_segment(column_id);
        // std::cerr << "column_id : " << column_id << " chunk id : " << chunk_id << " ( size: " << chunk->size() << " and matrix size: " << row_ids_and_radix_values[chunk_id].size() << ")\n";

        resolve_data_type(segment->data_type(), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          auto row_id = size_t{0};
          segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
            const auto partition_id = !position.is_null() * std::hash<ColumnDataType>{}(position.value()) % _partition_counts[column_id_offset];
            auto& row_ids_and_radix_values_of_chunk = row_ids_and_radix_values[chunk_id];
            const auto write_offset = row_id * row_ids_and_radix_values_width;
            // TOOD: we write the "RowID" every time. Unnecessary.
            row_ids_and_radix_values_of_chunk[write_offset] = static_cast<uint32_t>(chunk_id);
            row_ids_and_radix_values_of_chunk[write_offset + 1] = static_cast<uint32_t>(position.chunk_offset());
            // std::cerr << "writing to offset " << write_offset + 2 + column_id_offset << "(((" << write_offset << " + 2 + " << column_id_offset << ")))\n";
            // DebugAssert((chunk->size() * row_ids_and_radix_values_width) > (write_offset + 2 + column_id_offset), "NARF"); 
            row_ids_and_radix_values_of_chunk[write_offset + 2 + column_id_offset] = static_cast<uint32_t>(partition_id);
            // std::cerr << "row_id is now " << row_id << "\n";
            ++row_id;
          });
        });

        // for (auto t : histogram_subspan) {
        //   std::cerr << t << " ";
        // }
        // std::cerr << "\n\n";
      }));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // for (auto chunk_id = ChunkID{0}; chunk_id < input_table_chunk_count; ++chunk_id) {
  //   std::cerr << "\n\n";
  //   for (auto offset = size_t{0}; offset < row_ids_and_radix_values[chunk_id].size(); ++offset) {
  //     if (offset % row_ids_and_radix_values_width == 0) {
  //       std::cerr << "\n| ";
  //     }
  //     std::cerr << row_ids_and_radix_values[chunk_id][offset] << " | ";
  //   }
  // }
  // std::cerr << "\n";

  // auto counter = size_t{0};
  // for (auto t : histogram) {
  //   if (counter % partition_count == 0) {
  //     std::cerr << "  ";
  //   }
  //   std::cerr << t << " ";
  //   ++counter;
  // }
  // std::cerr << "\n";

  

  /**
   * Phase #2: calculate histograms, followeg by prefix sums.
   */

  // TODO: this step could be skipped when shuffling on single column.
  jobs.clear();
  auto histogram = std::vector<size_t>(input_table_chunk_count * overall_partition_count);
  // std::cerr << "overall_partition_count is " << overall_partition_count << " and histogram size is " << histogram.size() << "\n";
  // std::cerr << std::format("Creating histogram of size {} ({} * {})\n", histogram.size(), static_cast<size_t>(input_chunk_count), partition_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_chunk_count; ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id] () {
      auto& row_ids_and_radix_values_of_chunk = row_ids_and_radix_values[chunk_id];
      const auto overall_cells = row_ids_and_radix_values_of_chunk.size();
      for (auto row_offset = size_t{0}; row_offset < overall_cells; row_offset += row_ids_and_radix_values_width) {
        auto partition_id = uint32_t{0};
        for (auto column_id_offset = size_t{0}; column_id_offset < partition_column_count; ++column_id_offset) {
          const auto read_offset = row_offset + 2 + column_id_offset;
          DebugAssert(read_offset < row_ids_and_radix_values_of_chunk.size(), "Narf");
          partition_id += row_ids_and_radix_values_of_chunk[read_offset] * column_factors[column_id_offset];
        }
        // std::cerr << "partitionID: " << partition_id << " and writing to " << chunk_id * overall_partition_count + partition_id << "\n";
        ++histogram[chunk_id * overall_partition_count + partition_id];
        row_ids_and_radix_values_of_chunk[row_offset + 2 + partition_column_count] = partition_id;
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // auto counter = size_t{0};
  // for (auto t : histogram) {
  //   if (counter % overall_partition_count == 0) {
  //     std::cerr << "  ";
  //   }
  //   std::cerr << t << " ";
  //   ++counter;
  // }
  // std::cerr << "\n";

  auto write_offsets = std::vector<size_t>(input_table_chunk_count * overall_partition_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_chunk_count - 1; ++chunk_id) {
    for (auto partition_id = size_t{0}; partition_id < overall_partition_count; ++partition_id) {
      // auto histogram_subspan = std::span(histogram).subspan(chunk_id * overall_partition_count, overall_partition_count);
      // Strided jumping per partition.
      // Or per chunk? That should read more data per cache line. << Taking that.
      write_offsets[(chunk_id + 1) * overall_partition_count + partition_id] = histogram[chunk_id * overall_partition_count + partition_id] +
                                                                               write_offsets[chunk_id * overall_partition_count + partition_id];
    }
  }

  // counter = 0;
  // for (auto t : write_offsets) {
  //   if (counter % overall_partition_count == 0) {
  //     std::cerr << "  ";
  //   }
  //   std::cerr << t << " ";
  //   ++counter;
  // }
  // std::cerr << "\n";

  // for (auto chunk_id = ChunkID{0}; chunk_id < input_table_chunk_count; ++chunk_id) {
  //   std::cerr << "\n\n";
  //   for (auto offset = size_t{0}; offset < row_ids_and_radix_values[chunk_id].size(); ++offset) {
  //     if (offset % row_ids_and_radix_values_width == 0) {
  //       std::cerr << "\n| ";
  //     }
  //     std::cerr << row_ids_and_radix_values[chunk_id][offset] << " | ";
  //   }
  // }
  // std::cerr << "\n";

  /**
   * Phase #3: shuffle rows.
   */
  auto partitioned_pos_lists = std::vector<std::shared_ptr<RowIDPosList>>(overall_partition_count);
  for (auto partition_id = size_t{0}; partition_id < overall_partition_count; ++partition_id) {
    auto& pos_list_vector = partitioned_pos_lists[partition_id];
    pos_list_vector = std::make_shared<RowIDPosList>(histogram[(input_table_chunk_count - 1) * overall_partition_count + partition_id] +
                                                     write_offsets[(input_table_chunk_count - 1) * overall_partition_count + partition_id]);
    // pos_list_vector.resize(histogram[(input_table_chunk_count - 1) * overall_partition_count + partition_id] +
                           // write_offsets[(input_table_chunk_count - 1) * overall_partition_count + partition_id]);
    // std::cerr << "resized to " << pos_list_vector->size() << "\n";
  }

  jobs.clear();
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_chunk_count; ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id] () {
      auto write_offsets_subspan = std::span(write_offsets).subspan(chunk_id * overall_partition_count, overall_partition_count);
      auto local_write_offsets = std::vector<size_t>{};
      local_write_offsets.insert(local_write_offsets.begin(), write_offsets_subspan.begin(), write_offsets_subspan.end());

      const auto& chunk = input_table->get_chunk(chunk_id);
      if (!chunk) {
        return;
      }

      const auto& row_ids_and_radix_values_of_chunk = row_ids_and_radix_values[chunk_id];
      const auto overall_cells = row_ids_and_radix_values_of_chunk.size();
      for (auto row_offset = size_t{0}; row_offset < overall_cells; row_offset += row_ids_and_radix_values_width) {
        const auto item_chunk_id = row_ids_and_radix_values_of_chunk[row_offset];
        const auto item_chunk_offset = row_ids_and_radix_values_of_chunk[row_offset + 1];
        const auto item_partition_id = row_ids_and_radix_values_of_chunk[row_offset + row_ids_and_radix_values_width - 1];
        (*partitioned_pos_lists[item_partition_id])[local_write_offsets[item_partition_id]] = {ChunkID{item_chunk_id}, ChunkOffset{item_chunk_offset}};
        ++local_write_offsets[item_partition_id];
      }

      // for (auto t : histogram_subspan) {
      //   std::cerr << t << " ";
      // }
      // std::cerr << "\n\n";
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);


  /**
   * Phase #4: Build output table
   */
  const auto column_count = input_table->column_count();
  const auto pos_list_count = partitioned_pos_lists.size();
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>(pos_list_count);
  for (auto pos_list_id = size_t{0}; pos_list_id < pos_list_count; ++pos_list_id) {
    const auto& pos_list = partitioned_pos_lists[pos_list_id];
    if (pos_list->empty()) {
      continue;
    }
    auto segments = Segments{};
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      segments.emplace_back(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }
    output_chunks[pos_list_id] = std::make_shared<Chunk>(segments);
  }
  auto output_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References, output_chunks);

  // Print::print(input_table);
  // Print::print(output_table);

  return output_table;
}
















// std::shared_ptr<const Table> Shuffle::_on_execute_old() {
//   auto timer = Timer{};
//   const auto& initial_input_table = left_input_table();
//   auto table_to_partition = initial_input_table;
//   auto input_chunk_count = input_table->chunk_count();
//   Assert(input_table->type() == TableType::Data, "Shuffle cannot be executed on reference tables.");

//   /**
//    *
//    *
//    * Main Phase #1: partition unclustered input table.
//    * 
//    * 
//    */

//   /**
//    * Phase #1.1: building histogram.
//    */
//   auto partition_count = _partition_counts[0];
//   std::cerr << "chunk count " << table_to_partition->chunk_count() << "\n";
//   auto histogram = std::vector<size_t>(input_chunk_count * partition_count);
//   std::cerr << std::format("Creating histogram of size {} ({} * {})\n", histogram.size(), static_cast<size_t>(input_chunk_count), partition_count);

//   auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
//   jobs.reserve(input_chunk_count);
//   for (auto chunk_id = ChunkID{0}; chunk_id < input_chunk_count; ++chunk_id) {
//     jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id] () {
//       auto histogram_subspan = std::span(histogram).subspan(chunk_id * partition_count, partition_count);

//       // for (auto t : histogram_subspan) {
//       //   std::cerr << t << " ";
//       // }
//       // std::cerr << std::endl;

//       const auto& chunk = table_to_partition->get_chunk(chunk_id);
//       if (!chunk) {
//         return;
//       }

//       const auto& segment = chunk->get_segment(_column_ids[0]);

//       resolve_data_type(segment->data_type(), [&](auto type) {
//         using ColumnDataType = typename decltype(type)::type;
//         segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
//           const auto partition_id = !position.is_null() * std::hash<ColumnDataType>{}(position.value()) % partition_count;
//           ++histogram_subspan[partition_id];
//           // std::cerr << "hash is " << std::hash<ColumnDataType>{}(position.value()) << " and update: " << std::hash<ColumnDataType>{}(position.value()) % partition_count << "\n";
//         });
//       });

//       // for (auto t : histogram_subspan) {
//       //   std::cerr << t << " ";
//       // }
//       // std::cerr << "\n\n";
//     }));
//   }
//   Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

//   auto counter = size_t{0};
//   for (auto t : histogram) {
//     if (counter % partition_count == 0) {
//       std::cerr << "  ";
//     }
//     std::cerr << t << " ";
//     ++counter;
//   }
//   std::cerr << "\n";

//   /**
//    * Phase #1.2: calculate prefix sums and shuffle.
//    */
//   auto write_offsets = std::vector<size_t>(input_chunk_count * partition_count);
//   for (auto chunk_id = ChunkID{0}; chunk_id < input_chunk_count - 1; ++chunk_id) {
//     for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
//       // Strided jumping per partition.
//       // Or per chunk? That should read more data per cache line. << Taking that.
//       write_offsets[(chunk_id + 1) * partition_count + partition_id] = histogram[chunk_id * partition_count + partition_id] +
//                                                                             write_offsets[chunk_id * partition_count + partition_id];
//     }
//   }

//   counter = 0;
//   for (auto t : write_offsets) {
//     if (counter % partition_count == 0) {
//       std::cerr << "  ";
//     }
//     std::cerr << t << " ";
//     ++counter;
//   }
//   std::cerr << "\n";

//   auto partitioned_pos_lists = std::vector<std::shared_ptr<RowIDPosList>>(partition_count);

//   for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
//     auto& pos_list_vector = partitioned_pos_lists[partition_id];
//     pos_list_vector = std::make_shared<RowIDPosList>(histogram[(input_chunk_count - 1) * partition_count + partition_id] +
//                            write_offsets[(input_chunk_count - 1) * partition_count + partition_id]);
//     // pos_list_vector.resize(histogram[(input_chunk_count - 1) * partition_count + partition_id] +
//                            // write_offsets[(input_chunk_count - 1) * partition_count + partition_id]);
//     // std::cerr << "resized to " << pos_list_vector->size() << "\n";
//   }

//   jobs.clear();
//   for (auto chunk_id = ChunkID{0}; chunk_id < input_chunk_count; ++chunk_id) {
//     jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id] () {
//       auto write_offsets_subspan = std::span(write_offsets).subspan(chunk_id * partition_count, partition_count);
//       auto local_write_offsets = std::vector<size_t>{};
//       local_write_offsets.insert(local_write_offsets.begin(), write_offsets_subspan.begin(), write_offsets_subspan.end());

//       const auto& chunk = table_to_partition->get_chunk(chunk_id);
//       if (!chunk) {
//         return;
//       }

//       const auto& segment = chunk->get_segment(_column_ids[0]);

//       resolve_data_type(segment->data_type(), [&](auto type) {
//         using ColumnDataType = typename decltype(type)::type;
//         segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
//           const auto partition_id = !position.is_null() * std::hash<ColumnDataType>{}(position.value()) % partition_count;

//           (*partitioned_pos_lists[partition_id])[local_write_offsets[partition_id]] = {chunk_id, position.chunk_offset()};
//           ++local_write_offsets[partition_id];
//         });
//       });

//       // for (auto t : histogram_subspan) {
//       //   std::cerr << t << " ";
//       // }
//       // std::cerr << "\n\n";
//     }));
//   }
//   Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);


//   /**
//    * Phase #1.3: Build output table
//    */
//   const auto column_count = table_to_partition->column_count();
//   auto output_table = std::make_shared<Table>(table_to_partition->column_definitions(), TableType::References);
//   for (const auto& pos_list : partitioned_pos_lists) {
//     auto segments = Segments{};
//     for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
//       segments.emplace_back(std::make_shared<ReferenceSegment>(table_to_partition, column_id, pos_list));
//     }
//     output_table->append_chunk(segments);
//   }

//   table_to_partition = output_table;

//   /**
//    *
//    *
//    * Main Phase #2: partition intermediate tables (clustered per chunk).
//    * 
//    * 
//    */

//   for (auto shuffle_partition_id = size_t{1}; shuffle_partition_id < _partition_counts.size(); ++shuffle_partition_id) {
//     auto partition_count = _partition_counts[shuffle_partition_id];
//     input_table = output_table;
//     const auto input_table_column_count = input_table->column_count();
//     const auto chunk_count = input_table->chunk_count();
//     std::cerr << "chunk count " << input_table->chunk_count() << "\n";
//     auto histogram = std::vector<size_t>(chunk_count * partition_count);
//     std::cerr << std::format("Creating histogram of size {} ({} * {})\n", histogram.size(), static_cast<size_t>(chunk_count), partition_count);

//     auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
//     auto output_chunks = std::vector<std::shared_ptr<Chunk>>{input_chunk_count * partition_count};
//     for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
//       jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id] () {
//         const auto& chunk = input_table->get_chunk(chunk_id);
//         if (!chunk) {
//           return;
//         }

//         const auto average_partition_size = chunk->size() / partition_count;
//         auto partitioned_pos_lists = std::vector<std::shared_ptr<RowIDPosList>>(partition_count);
//         for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
//           partitioned_pos_lists[partition_id] = std::make_shared<RowIDPosList>();
//           partitioned_pos_lists[partition_id]->reserve(average_partition_size);
//         }

//         const auto& segment = chunk->get_segment(_column_ids[shuffle_partition_id]);

//         resolve_data_type(segment->data_type(), [&](auto type) {
//           using ColumnDataType = typename decltype(type)::type;
//           segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
//             const auto partition_id = !position.is_null() * std::hash<ColumnDataType>{}(position.value()) % partition_count;

//             partitioned_pos_lists[partition_id]->emplace_back(chunk_id, position.chunk_offset());
//           });
//         });

//         for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
//           auto& partitioned_pos_list = partitioned_pos_lists[partition_id];
//           std::cerr << "Size of PosList: " << partitioned_pos_list->size() << std::endl;

//           auto segments = Segments{};
//           for (auto column_id = ColumnID{0}; column_id < input_table_column_count; ++column_id) {
//             segments.emplace_back(std::make_shared<ReferenceSegment>(initial_input_table, column_id, partitioned_pos_list));
//           }
//           output_chunks[chunk_id * partition_count + partition_id] = std::make_shared<Chunk>(segments);
//         }

//       }));
//     }
//     Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

//     /**
//      * Phase #3: Build output table
//      */
//     auto output_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References, output_chunks);
//     // for (const auto& segments : output_chunks) {
//     //   output_table->append_chunk(segments);
//     // }

//     input_table = output_table;
//   }

//   // Print::print(input_table);
//   // Print::print(output_table);

//   // Per chunk, we collect a vector of deques (size is number of partitions)
//   // auto intermediate_results = std::deque<IntermediateShufflePartition>{};
//   // auto final_results = std::vector<std::vector<RowID>>(partition_count);

//   // const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
//   // Assert(node_queue_scheduler, "NodeQueueScheduler not set?");
//   // Assert(node_queue_scheduler->queues().size() == 1, "Unexpected NUMA topology.");

//   //   jobs.emplace_back(std::make_shared<JobTask>(
//   //       [&, chunk]() {
//   //         shuffle_chunk(chunk, intermediate_results, _columns, _partition_counts);

//   //         if (merge_semaphore.try_acquire()) {
//   //           merge_intermediate_results(intermediate_results, final_results);
//   //           merge_semaphore.release();
//   //         }
//   //       },
//   //       SchedulePriority::Default));
//   //   jobs.back()->schedule();
//   // }

//   // Hyrise::get().scheduler()->wait_for_tasks(jobs);

//   // Assert(_input_chunk_sink->_chunks_added == _input_chunk_sink->_chunks_pulled, "error #1");
//   // Assert(_input_chunk_sink->_chunks_pulled == _output_chunk_sink->_chunks_added, "error #2");
//   // Assert(!_input_chunk_sink->pull_chunk(), "error #3: Able to pull a job even though sink is finished.");

//   // return std::make_shared<Table>(input_table->column_definitions(), TableType::References);
//   return input_table;
// }

}  // namespace hyrise
