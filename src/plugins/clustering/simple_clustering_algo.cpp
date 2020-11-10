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

SimpleClusteringAlgo::SimpleClusteringAlgo(ClusteringByTable clustering) : AbstractClusteringAlgo(clustering) {}

const std::string SimpleClusteringAlgo::description() const {
  return "SimpleClusteringAlgo";
}

std::shared_ptr<Table> SimpleClusteringAlgo::_sort_table_mutable(const std::shared_ptr<Table> table, const std::string& column_name, const ChunkOffset chunk_size){
  // We sort the tables after their creation so that we are independent of the order in which they are filled.
  // For this, we use the sort operator. Because it returns a `const Table`, we need to recreate the table and
  // migrate the sorted chunks to that table.

  const auto sort_mode = SortMode::Ascending;
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  const std::vector<SortColumnDefinition> sort_column_definitions = { SortColumnDefinition(table->column_id_by_name(column_name), sort_mode) };
  auto sort = std::make_shared<Sort>(table_wrapper, sort_column_definitions, chunk_size, Sort::ForceMaterialization::Yes);
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
    const auto sort_mode = SortMode::Ascending;
    auto table_wrapper = std::make_shared<TableWrapper>(new_table);
    table_wrapper->execute();
    const std::vector<SortColumnDefinition> sort_column_definitions = { SortColumnDefinition(table->column_id_by_name(column_name), sort_mode) };
    auto sort = std::make_shared<Sort>(table_wrapper, sort_column_definitions, sort_chunk_size, Sort::ForceMaterialization::Yes);
    sort->execute();
    const auto sorted_table = sort->get_output();

    _append_chunks(sorted_table, clustered_table);
  }

  return clustered_table;
}

void SimpleClusteringAlgo::_append_chunks(const std::shared_ptr<const Table> from, std::shared_ptr<Table> to) {
  Assert(!from->get_chunk(ChunkID{0})->individually_sorted_by().empty(), "from table needs to be sorted");

  const auto chunk_count = from->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = from->get_chunk(chunk_id);
    _append_chunk(chunk, to);
  }
}

void SimpleClusteringAlgo::_append_chunk(const std::shared_ptr<const Chunk> chunk, std::shared_ptr<Table> to) {
  Assert(!chunk->individually_sorted_by().empty(), "chunk needs to be sorted");

  const auto column_count = chunk->column_count();
  const auto& sorted_by = chunk->individually_sorted_by();

  auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
  Segments segments{};
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    segments.emplace_back(chunk->get_segment(column_id));
  }

  {
    const auto append_lock = to->acquire_append_mutex();
    to->append_chunk(segments, mvcc_data);
    to->last_chunk()->finalize();
    to->last_chunk()->set_individually_sorted_by(sorted_by);
  }
}

void SimpleClusteringAlgo::_perform_clustering() {
  // TODO do we need a timer here?
  auto& storage_manager = Hyrise::get().storage_manager;
  Timer timer;

  if (!clustering_by_table.empty()) {
    std::cout << "[" << description() << "] " << "Clustering tables" << std::endl;

    for (const auto& [table_name, clustering_columns] : clustering_by_table) {
      Assert(clustering_columns.size() >= 1, "you have to specify at least one clustering dimension, otherwise just leave out the table entry");

      Assert(storage_manager.has_table(table_name), "clustering contains an entry for " + table_name + ", but no such table exists");
      const auto original_table = storage_manager.get_table(table_name);
      std::cout << "[" << description() << "] " << " Clustering " << table_name << std::endl;

      uint64_t expected_final_chunk_count = 1;
      for (const auto& [column_name, num_groups] : clustering_columns) {
        expected_final_chunk_count *= num_groups;
      }
      Assert(expected_final_chunk_count < original_table->row_count(), "cannot have more chunks than rows");
      constexpr auto MIN_REASONABLE_CHUNK_SIZE = 5;
      Assert(MIN_REASONABLE_CHUNK_SIZE * expected_final_chunk_count < original_table->row_count(), "chunks in " + table_name + " will have less than " + std::to_string(MIN_REASONABLE_CHUNK_SIZE) + " rows");

      const auto initial_table_size = original_table->row_count();
      Timer per_clustering_timer;

      // Idea: We start with "1" chunk (whole table).
      // With each step, we split chunks, further clustering their contents.
      // This means that for the first clustering column, we sort the whole table,
      // while for all subsequent clustering columns, we only sort on chunk level

      // first clustering column
      const auto& first_column_name = clustering_columns[0].first;
      const auto first_column_desired_chunk_count = clustering_columns[0].second;
      const auto first_column_chunksize = static_cast<ChunkOffset>(std::ceil(1.0 * original_table->row_count() / first_column_desired_chunk_count));
      std::cout << "[" << description() << "] " << "  Clustering '" << table_name << "' by '" << first_column_name << "', split up is " << first_column_desired_chunk_count  << " " << std::flush;

      auto mutable_sorted_table = _sort_table_mutable(original_table, first_column_name, first_column_chunksize);
      std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;


      // all subsequent clustering columns
      for (auto clustering_column_index = 1u;clustering_column_index < clustering_columns.size();clustering_column_index++) {
        const auto& column_name = clustering_columns[clustering_column_index].first;
        const auto desired_chunk_count = clustering_columns[clustering_column_index].second;

        if (desired_chunk_count == 1) {
          std::cout << "[" << description() << "] " << "  Sorting '" << table_name << "' on chunk level by '" << column_name << "' " << std::flush;
        } else {
          std::cout << "[" << description() << "] " << "  Clustering '" << table_name << "' by '" << column_name << "', split up is " << desired_chunk_count << " " << std::flush;
        }

        mutable_sorted_table = _sort_table_chunkwise(mutable_sorted_table, column_name, desired_chunk_count);
        std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;
      }


      // copy constraints, TPCHBenchmarkItemRunner complains otherwise
      std::cout << "[" << description() << "] " << "  Adding unique constraints to " << table_name << " " << std::flush;
      for (const auto &constraint : original_table->soft_key_constraints()) {
        mutable_sorted_table->add_soft_key_constraint(constraint);
      }
      std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;

      // finalize all chunks, then perform encoding (currently fixed to Dictionary)
      std::cout << "[" << description() << "] " << "  Applying Dictionary encoding to " << table_name << " " << std::flush;
      for (auto chunk_id = ChunkID{0}; chunk_id < mutable_sorted_table->chunk_count(); ++chunk_id) {
        const auto chunk = mutable_sorted_table->get_chunk(chunk_id);
        if (chunk->is_mutable()) chunk->finalize();
      }
      ChunkEncoder::encode_all_chunks(mutable_sorted_table, SegmentEncodingSpec{EncodingType::Dictionary});
      std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;

      // add table
      std::cout << "[" << description() << "] " << "  Adding " << table_name << " again " << std::flush;
      storage_manager.drop_table(table_name);
      storage_manager.add_table(table_name, mutable_sorted_table);
      std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;

      const auto final_table_size = mutable_sorted_table->row_count();
      Assert(initial_table_size == final_table_size, "Size of table " + table_name + " changed during clustering");
    }
    // TODO
    // metrics.sort_duration = timer.lap();
    //std::cout << "- Sorting tables done (" << format_duration(metrics.sort_duration) << ")" << std::endl;
  }

  std::cout << "[" << description() << "] Clustering completed, running assertions" << std::endl;
  _run_assertions();
}

} // namespace opossum
