#include "simple_clustering_algo.hpp"

#include <chrono>
#include <memory>

#include "abstract_clustering_algo.hpp"
#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace opossum {

SimpleClusteringAlgo::SimpleClusteringAlgo(StorageManager& storage_manager, ClusteringByTable clustering) : AbstractClusteringAlgo(storage_manager, clustering) {}

const std::string SimpleClusteringAlgo::description() const {
  return "SimpleClusteringAlgo";
}

std::shared_ptr<Table> SimpleClusteringAlgo::_sort_table_mutable(const std::shared_ptr<Table> table, const std::string& column_name, const ChunkOffset chunk_size){
  // We sort the tables after their creation so that we are independent of the order in which they are filled.
  // For this, we use the sort operator. Because it returns a `const Table`, we need to recreate the table and
  // migrate the sorted chunks to that table.

  const auto order_by_mode = OrderByMode::Ascending;
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto sort = std::make_shared<Sort>(table_wrapper, table->column_id_by_name(column_name), order_by_mode,
                                     chunk_size);
  sort->execute();
  const auto immutable_sorted_table = sort->get_output();
  auto result = std::make_shared<Table>(immutable_sorted_table->column_definitions(), TableType::Data,
                                  chunk_size, UseMvcc::Yes);
  _append_chunks(immutable_sorted_table, result);

  return result;
}

std::shared_ptr<Table> SimpleClusteringAlgo::_sort_table_chunkwise(const std::shared_ptr<const Table> table, const std::string& column_name, const uint64_t desired_chunk_split_count) {
  auto clustered_table = std::make_shared<Table>(table->column_definitions(), TableType::Data,
                            std::nullopt, UseMvcc::Yes);

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    //std::cout << "sorting chunk " << chunk_id << std::endl;

    const auto chunk = table->get_chunk(chunk_id);
    const auto chunk_row_count = chunk->size();
    const auto sort_chunk_size = static_cast<ChunkOffset>(std::ceil(1.0 * chunk_row_count / desired_chunk_split_count));

    auto new_table = std::make_shared<Table>(table->column_definitions(), TableType::Data,
                            sort_chunk_size, UseMvcc::Yes);
    _append_chunk(chunk, new_table);


    // We could simply use _sort_table_mutable(), but that would create another copy of the table to get rid of immutability
    const auto order_by_mode = OrderByMode::Ascending;
    auto table_wrapper = std::make_shared<TableWrapper>(new_table);
    table_wrapper->execute();
    auto sort = std::make_shared<Sort>(table_wrapper, table->column_id_by_name(column_name), order_by_mode, sort_chunk_size);
    sort->execute();
    const auto sorted_table = sort->get_output();

    _append_chunks(sorted_table, clustered_table);
  }

  return clustered_table;
}

void SimpleClusteringAlgo::_append_chunks(const std::shared_ptr<const Table> from, std::shared_ptr<Table> to) {
  Assert(from->get_chunk(ChunkID{0})->ordered_by(), "from table needs to be sorted");

  const auto chunk_count = from->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = from->get_chunk(chunk_id);
    _append_chunk(chunk, to);
  }
}

void SimpleClusteringAlgo::_append_chunk(const std::shared_ptr<const Chunk> chunk, std::shared_ptr<Table> to) {
  Assert(chunk->ordered_by(), "chunk needs to be sorted");

  const auto column_count = chunk->column_count();
  const auto& ordered_by = *(chunk->ordered_by());

  auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
  Segments segments{};
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    segments.emplace_back(chunk->get_segment(column_id));
  }
  to->append_chunk(segments, mvcc_data);
  to->last_chunk()->set_ordered_by(ordered_by);
}

void SimpleClusteringAlgo::run() {
  // TODO do we need a timer here?
  Timer timer;

  if (!clustering_by_table.empty()) {
    std::cout << "- Sorting tables" << std::endl;

    for (const auto& [table_name, clustering_columns] : clustering_by_table) {
      Assert(clustering_columns.size() >= 1, "you have to specify at least one clustering dimension, otherwise just leave out the table entry");

      Assert(storage_manager.has_table(table_name), "clustering contains an entry for " + table_name + ", but no such table exists");
      const auto original_table = storage_manager.get_table(table_name);

      uint64_t expected_final_chunk_count = 1;
      for (const auto& [column_name, num_groups] : clustering_columns) {
        expected_final_chunk_count *= num_groups;
      }
      Assert(expected_final_chunk_count < original_table->row_count(), "cannot have more chunks than rows");
      constexpr auto MIN_REASONABLE_CHUNK_SIZE = 5;
      Assert(MIN_REASONABLE_CHUNK_SIZE * expected_final_chunk_count < original_table->row_count(), "chunks in " + table_name + " will have less than " + std::to_string(MIN_REASONABLE_CHUNK_SIZE) + " rows");

      std::cout << "initial table size of " << table_name << " is: " << original_table->row_count() << std::endl;
      Timer per_clustering_timer;

      // Idea: We start with "1" chunk (whole table).
      // With each step, we split chunks, further clustering their contents.
      // This means that for the first clustering column, we sort the whole table,
      // while for all subsequent clustering columns, we only sort on chunk level

      // first clustering column
      const auto& first_column_name = clustering_columns[0].first;
      const auto first_column_chunksize = static_cast<ChunkOffset>(std::ceil(1.0 * original_table->row_count() / clustering_columns[0].second));
      std::cout << "-  Clustering '" << table_name << "' by '" << first_column_name << "', split up is " << first_column_chunksize  << " " << std::flush;

      auto mutable_sorted_table = _sort_table_mutable(original_table, first_column_name, first_column_chunksize);
      std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;


      // all subsequent clustering columns
      for (auto clustering_column_index = 1u;clustering_column_index < clustering_columns.size();clustering_column_index++) {
        const auto& column_name = clustering_columns[clustering_column_index].first;
        const auto desired_chunk_count = clustering_columns[clustering_column_index].second;

        if (desired_chunk_count == 1) {
          std::cout << "-  Sorting '" << table_name << "' on chunk level by '" << column_name << "' " << std::flush;
        } else {
          std::cout << "-  Clustering '" << table_name << "' by '" << column_name << "', split up is " << desired_chunk_count << " " << std::flush;
        }

        mutable_sorted_table = _sort_table_chunkwise(mutable_sorted_table, column_name, desired_chunk_count);
        std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;
      }

      // TODO
      // metrics.chunk_order_by_table = chunk_order_by_table;


      // copy constraints, TPCHBenchmarkItemRunner complains otherwise
      for (const auto &constraint : original_table->get_soft_unique_constraints()) {
        mutable_sorted_table->add_soft_unique_constraint(constraint.columns, constraint.is_primary_key);
      }


      // finalize all chunks, then perform encoding (currently fixed to Dictionary)
      for (auto chunk_id = ChunkID{0}; chunk_id < mutable_sorted_table->chunk_count(); ++chunk_id) {
        const auto chunk = mutable_sorted_table->get_chunk(chunk_id);
        if (chunk->is_mutable()) chunk->finalize();
      }
      ChunkEncoder::encode_all_chunks(mutable_sorted_table, EncodingType::Dictionary);

      // add table
      storage_manager.drop_table(table_name);
      storage_manager.add_table(table_name, mutable_sorted_table);

      std::cout << "final table size of " << table_name << " is: " << mutable_sorted_table->row_count() << std::endl;
    }
    // TODO
    // metrics.sort_duration = timer.lap();
    //std::cout << "- Sorting tables done (" << format_duration(metrics.sort_duration) << ")" << std::endl;
  }

  std::cout << "[" << description() << "] Clustering completed, running assertions" << std::endl;
  _run_assertions();
}

void SimpleClusteringAlgo::_run_assertions() const {
  for (const auto& table_name : Hyrise::get().storage_manager.table_names()) {
    const auto table = Hyrise::get().storage_manager.get_table(table_name);
    Assert(table, "table named \"" + table_name + "\" disappeared");

    // Clustering config for this table
    std::vector<std::pair<std::string, uint32_t>> clustering_columns {};
    if (clustering_by_table.find(table_name) != clustering_by_table.end()) {
      clustering_columns = clustering_by_table.at(table_name);
    }

    // Determine whether and by which column the table should be ordered
    std::optional<ColumnID> ordered_by_column_id;
    if (!clustering_columns.empty()) {
      uint64_t expected_final_chunk_count = 1;
      for (const auto& clustering_entry : clustering_columns) {
        expected_final_chunk_count *= clustering_entry.second;
      }
      Assert(table->chunk_count() == expected_final_chunk_count, "expected " + std::to_string(expected_final_chunk_count)
        + " chunks, but " + table_name + " got " + std::to_string(table->chunk_count()));

      auto sorted_column_name = clustering_columns.back().first;
      ordered_by_column_id = table->column_id_by_name(sorted_column_name);
    }
    std::cout << "[" << description() << "] " << "- Chunk count is correct" << " for table " << table_name << std::endl;

    // Determine the target chunk size.
    // TODO this is hacky, but this class does not know about benchmark_config->chunk_size, and not sure if it should
    ChunkOffset target_chunk_size;
    if (Hyrise::get().storage_manager.has_table("lineitem")) {
      target_chunk_size = 25'100;
    } else if (Hyrise::get().storage_manager.has_table("customer_demographics")) {
      target_chunk_size = Chunk::DEFAULT_SIZE;
    } else {
      Fail("Please provide a default chunk size for this benchmark");
    }


    // Iterate over all chunks, and check that ...
    for (ChunkID chunk_id{0};chunk_id < table->chunk_count();chunk_id++) {
      const auto chunk = table->get_chunk(chunk_id);
      Assert(chunk, "could not get chunk with id " + std::to_string(chunk_id));

      // ... ordering information is as expected
      Assert(!chunk->is_mutable(), "chunk is still mutable");
      if (ordered_by_column_id) {
        Assert(chunk->ordered_by(), "chunk should be ordered by " + table->column_name(*ordered_by_column_id) + ", but is unordered");
        Assert((*chunk->ordered_by()).first == *ordered_by_column_id, "chunk should be ordered by " + table->column_name(*ordered_by_column_id) 
          + ", but is ordered by " + table->column_name((*chunk->ordered_by()).first));
      } else {
        Assert(!chunk->ordered_by(), "chunk should be unordered, but is ordered by " + table->column_name((*chunk->ordered_by()).first));
      }
      std::cout << "[" << description() << "] " << "- Ordering information is correct" << " for table " << table_name << std::endl;

      // ... chunks are actually ordered according to the ordering information
      if (ordered_by_column_id) {
        auto is_sorted = true;
        const auto sort_column_id = *ordered_by_column_id;
        resolve_data_type(table->column_data_type(sort_column_id), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          auto last_value = std::optional<ColumnDataType>{};
          const auto& segment = table->get_chunk(chunk_id)->get_segment(sort_column_id);
          segment_with_iterators<ColumnDataType>(*segment, [&](auto it, const auto end) {
            while (it != end) {
              if (it->is_null()) {
                if (last_value) {
                  // NULLs should come before all values
                  is_sorted = false;
                  break;
                } else {
                  it++;
                  continue;
                }
              }

              if (!last_value || it->value() >= *last_value) {
                last_value = it->value();
              } else {
                is_sorted = false;
                break;
              }

              ++it;
            }
          });
        });

        Assert(is_sorted, "segment should be sorted by " + table->column_name(sort_column_id) + ", but it isn't");
        std::cout << "[" << description() << "] " << "- Segments are actually sorted" << " for table " << table_name << std::endl;
      }

      // ... all segments should have DictionaryEncoding
      for (ColumnID col_id{0}; col_id < table->column_count();col_id++) {
        const auto& segment = chunk->get_segment(col_id);
        Assert(segment, "could not get segment with column id " + std::to_string(col_id));
        const auto encoding_spec = get_segment_encoding_spec(segment);
        Assert(encoding_spec.encoding_type == EncodingType::Dictionary, "segment is not dictionary-encoded");
      }
      std::cout << "[" << description() << "] " << "- All segments are DictionarySegments" << std::endl;

      // ... chunks have at most the target chunk size
      Assert(chunk->size() <= target_chunk_size, "chunk size should be <= " + std::to_string(target_chunk_size)
        + ", but is " + std::to_string(chunk->size()));
      std::cout << "[" << description() << "] " << "- All chunks have about the expected size" << " for table " << table_name << std::endl;
    }
  }
}

} // namespace opossum

