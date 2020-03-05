#include "abstract_table_generator.hpp"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <cmath>

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "hyrise.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace opossum {

void to_json(nlohmann::json& json, const TableGenerationMetrics& metrics) {
  json = {{"generation_duration", metrics.generation_duration.count()},
          {"encoding_duration", metrics.encoding_duration.count()},
          {"binary_caching_duration", metrics.binary_caching_duration.count()},
          {"sort_duration", metrics.sort_duration.count()},
          {"store_duration", metrics.store_duration.count()},
          {"index_duration", metrics.index_duration.count()},
          {"sort_order_by_table", metrics.sort_order_by_table}};
}

BenchmarkTableInfo::BenchmarkTableInfo(const std::shared_ptr<Table>& table) : table(table) {}

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : _benchmark_config(benchmark_config) {}

std::shared_ptr<Table> AbstractTableGenerator::_sort_table(const std::shared_ptr<Table> table, const std::string& column_name, const ChunkOffset chunk_size){
  // We sort the tables after their creation so that we are independent of the order in which they are filled.
  // For this, we use the sort operator. Because it returns a `const Table`, we need to recreate the table and
  // migrate the sorted chunks to that table.

  const auto order_by_mode = OrderByMode::Ascending;
  //auto& table = table_info_by_name[table_name].table;
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

void AbstractTableGenerator::_append_chunks(const std::shared_ptr<const Table> from, std::shared_ptr<Table> to) {
  Assert(from->get_chunk(ChunkID{0})->ordered_by(), "from table needs to be sorted");
  auto ordered_by = *(from->get_chunk(ChunkID{0})->ordered_by());
  const auto chunk_count = from->chunk_count();
  const auto column_count = from->column_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = from->get_chunk(chunk_id);
    auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
    Segments segments{};
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      segments.emplace_back(chunk->get_segment(column_id));
    }
    to->append_chunk(segments, mvcc_data);
    to->last_chunk()->set_ordered_by(ordered_by);
  }
}

void AbstractTableGenerator::generate_and_store() {
  Timer timer;

  std::cout << "- Loading/Generating tables " << std::endl;
  auto table_info_by_name = generate();
  metrics.generation_duration = timer.lap();
  std::cout << "- Loading/Generating tables done (" << format_duration(metrics.generation_duration) << ")" << std::endl;

  /**
   * Sort tables if a sort order was defined by the benchmark
   */

  auto sort_order_by_table = _sort_order_by_table();

//#############################################TODO
//  auto env_chunk_sort_order = std::getenv("CHUNK_SORT_COLUMN");
//#############################################TODO

  auto env_clustering_order = std::getenv("CLUSTERING_ORDER");
  if (env_clustering_order != nullptr) {
    // read table sorting configuration from environment variable
    // format: "<table1>:<column1>,<column2>;<table2>:<column1>,<column2>;..."
    // atm we can only sort (cluster) after 1 column per table
    const auto clustering_order = std::string(env_clustering_order);
    const auto clustering_json = nlohmann::json::parse(clustering_order);
    std::cout << "parsed clustering json:" << std::endl;
    std::cout << clustering_json.dump(2) << std::endl;

    SortOrderByTable result = clustering_json;
    sort_order_by_table = result;
  }

  metrics.sort_order_by_table = sort_order_by_table;

  if (!sort_order_by_table.empty()) {
    std::cout << "- Sorting tables" << std::endl;

    for (const auto& [table_name, clustering_columns] : sort_order_by_table) {
      Assert(clustering_columns.size() >= 1, "you have to specify at least one clustering dimension, otherwise just leave out the table entry");
      const auto original_table = table_info_by_name[table_name].table;

      uint64_t expected_final_chunk_count = 1;
      for (const auto& [column_name, num_groups] : clustering_columns) {
        expected_final_chunk_count *= num_groups;
      }
      Assert(expected_final_chunk_count < original_table->row_count(), "cannot have more chunks than rows");
      constexpr auto MIN_REASONABLE_CHUNK_SIZE = 500;
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
      std::cout << "-  Clustering '" << table_name << "' by '" << first_column_name << "' " << std::flush;

      auto mutable_sorted_table = _sort_table(original_table, first_column_name, first_column_chunksize);
      std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;



      for (auto clustering_column_index = 1u;clustering_column_index < clustering_columns.size();clustering_column_index++) {
        const auto& column_name = clustering_columns[clustering_column_index].first;
        const auto desired_chunk_count = clustering_columns[clustering_column_index].second;


        std::cout << "-  Clustering '" << table_name << "' by '" << column_name << "' " << std::flush;


        auto clustered_table = std::make_shared<Table>(original_table->column_definitions(), TableType::Data,
                                  std::nullopt, UseMvcc::Yes);


        for (ChunkID chunk_id{0}; chunk_id < mutable_sorted_table->chunk_count();chunk_id++) {
          //std::cout << "sorting chunk " << chunk_id << std::endl;

          const auto chunk = mutable_sorted_table->get_chunk(chunk_id);
          const auto chunk_row_count = chunk->size();
          const auto sort_chunk_size = static_cast<ChunkOffset>(std::ceil(1.0 * chunk_row_count / desired_chunk_count));

          auto new_table = std::make_shared<Table>(mutable_sorted_table->column_definitions(), TableType::Data,
                                  sort_chunk_size, UseMvcc::Yes);

          // append single chunk to the table
          auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
          Segments segments{};
          for (auto column_id = ColumnID{0}; column_id < new_table->column_count(); ++column_id) {
            segments.emplace_back(chunk->get_segment(column_id));
          }
          new_table->append_chunk(segments, mvcc_data);
          new_table->last_chunk()->set_ordered_by(*(mutable_sorted_table->last_chunk()->ordered_by()));
          // append single chunk end

          auto sorted_table = _sort_table(new_table, column_name, sort_chunk_size);
          _append_chunks(sorted_table, clustered_table);
        }

        mutable_sorted_table = clustered_table;
        std::cout << "(" << per_clustering_timer.lap_formatted() << ")" << std::endl;
      }

      table_info_by_name[table_name].table = mutable_sorted_table;
      std::cout << "final table size of " << table_name << " is: " << mutable_sorted_table->row_count() << std::endl;

      /*


      // if we cluster after 2 dimensions, first sort in 100k chunks, then cluster into the given chunksize
      // else directly sort into the given chunksize
      //const auto sort_chunk_size = (column_names.size() == 1) ? _benchmark_config->chunk_size : Chunk::DEFAULT_SIZE;
      const auto sort_chunk_size = _benchmark_config->chunk_size;
      auto mutable_sorted_table = _sort_table(table_info_by_name[table_name].table, column_names[0], sort_chunk_size);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;

      if (column_names.size() == 2) {
        // 2d clustering
        // sort chunkwise

        auto clustered_table = std::make_shared<Table>(table_info_by_name[table_name].table->column_definitions(), TableType::Data,
                                  _benchmark_config->chunk_size, UseMvcc::Yes);

        std::cout << "size of sorted table: " << mutable_sorted_table->row_count() << std::endl;

        std::cout << "-  Clustering '" << table_name << "' by '" << column_names[1] << "' " << std::flush;

        for (ChunkID chunk_id{0}; chunk_id < mutable_sorted_table->chunk_count();chunk_id++) {
          //std::cout << "sorting chunk " << chunk_id << std::endl;

          auto new_table = std::make_shared<Table>(mutable_sorted_table->column_definitions(), TableType::Data,
                                  sort_chunk_size, UseMvcc::Yes);

          // append single chunk to the table
          const auto chunk = mutable_sorted_table->get_chunk(chunk_id);
          auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
          Segments segments{};
          for (auto column_id = ColumnID{0}; column_id < new_table->column_count(); ++column_id) {
            segments.emplace_back(chunk->get_segment(column_id));
          }
          new_table->append_chunk(segments, mvcc_data);
          new_table->last_chunk()->set_ordered_by(*(mutable_sorted_table->last_chunk()->ordered_by()));
          // append single chunk end

          auto sorted_table = _sort_table(new_table, column_names[1], _benchmark_config->chunk_size);
          _append_chunks(sorted_table, clustered_table);
        }

        std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
        table_info_by_name[table_name].table = clustered_table;
      } else {
        table_info_by_name[table_name].table = mutable_sorted_table;
      }
      */


      /*
      for (auto it = column_names.rbegin(); it != column_names.rend(); ++it ) {
        const std::string column_name = *it;
        const auto order_by_mode = OrderByMode::Ascending;  // currently fixed to ascending
        std::cout << "-  Sorting '" << table_name << "' by '" << column_name << "' " << std::flush;
        Timer per_table_timer;
        // We sort the tables after their creation so that we are independent of the order in which they are filled.
        // For this, we use the sort operator. Because it returns a `const Table`, we need to recreate the table and
        // migrate the sorted chunks to that table.
        auto& table = table_info_by_name[table_name].table;
        auto table_wrapper = std::make_shared<TableWrapper>(table);
        table_wrapper->execute();
        auto sort = std::make_shared<Sort>(table_wrapper, table->column_id_by_name(column_name), order_by_mode,
                                           _benchmark_config->chunk_size);
        sort->execute();
        const auto immutable_sorted_table = sort->get_output();
        table = std::make_shared<Table>(immutable_sorted_table->column_definitions(), TableType::Data,
                                        Chunk::DEFAULT_SIZE, UseMvcc::Yes);
        const auto chunk_count = immutable_sorted_table->chunk_count();
        const auto column_count = immutable_sorted_table->column_count();
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = immutable_sorted_table->get_chunk(chunk_id);
          auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
          Segments segments{};
          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            segments.emplace_back(chunk->get_segment(column_id));
          }
          table->append_chunk(segments, mvcc_data);
          table->get_chunk(chunk_id)->set_ordered_by({table->column_id_by_name(column_name), order_by_mode});
        }
        std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
      }
      */
    }
    metrics.sort_duration = timer.lap();
    std::cout << "- Sorting tables done (" << format_duration(metrics.sort_duration) << ")" << std::endl;
  }

  /**
   * Add constraints if defined by the benchmark
   */
  _add_constraints(table_info_by_name);

  /**
   * Finalizing all chunks of all tables that are still mutable.
   */
  // TODO(any): Finalization might trigger encoding in the future.
  for (auto& [table_name, table_info] : table_info_by_name) {
    auto& table = table_info_by_name[table_name].table;
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);
      if (chunk->is_mutable()) chunk->finalize();
    }
  }

  /**
   * Encode the tables
   */
  std::cout << "- Encoding tables if necessary" << std::endl;
  for (auto& [table_name, table_info] : table_info_by_name) {
    std::cout << "-  Encoding '" << table_name << "' - " << std::flush;
    Timer per_table_timer;
    table_info.re_encoded =
        BenchmarkTableEncoder::encode(table_name, table_info.table, _benchmark_config->encoding_config);
    std::cout << (table_info.re_encoded ? "encoding applied" : "no encoding necessary");
    std::cout << " (" << per_table_timer.lap_formatted() << ")" << std::endl;
  }
  metrics.encoding_duration = timer.lap();
  std::cout << "- Encoding tables done (" << format_duration(metrics.encoding_duration) << ")" << std::endl;

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    for (auto& [table_name, table_info] : table_info_by_name) {
      const auto& table = table_info.table;
      if (table->chunk_count() > 1 && table->get_chunk(ChunkID{0})->size() != _benchmark_config->chunk_size) {
        std::cout << "- WARNING: " << table_name << " was loaded from binary, but has a mismatching chunk size of "
                  << table->get_chunk(ChunkID{0})->size() << std::endl;
      }
    }

    std::cout << "- Writing tables into binary files if necessary" << std::endl;

    for (auto& [table_name, table_info] : table_info_by_name) {
      if (table_info.loaded_from_binary && !table_info.re_encoded && !table_info.binary_file_out_of_date) {
        continue;
      }

      auto binary_file_path = std::filesystem::path{};
      if (table_info.binary_file_path) {
        binary_file_path = *table_info.binary_file_path;
      } else {
        binary_file_path = *table_info.text_file_path;
        binary_file_path.replace_extension(".bin");
      }

      std::cout << "- Writing '" << table_name << "' into binary file " << binary_file_path << " " << std::flush;
      Timer per_table_timer;
      BinaryWriter::write(*table_info.table, binary_file_path);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    metrics.binary_caching_duration = timer.lap();
    std::cout << "- Writing tables into binary files done (" << format_duration(metrics.binary_caching_duration) << ")"
              << std::endl;
  }

  /**
   * Add the Tables to the StorageManager
   */
  std::cout << "- Adding tables to StorageManager and generating statistics" << std::endl;
  auto& storage_manager = Hyrise::get().storage_manager;
  for (auto& [table_name, table_info] : table_info_by_name) {
    std::cout << "-  Adding '" << table_name << "' " << std::flush;
    Timer per_table_timer;
    if (storage_manager.has_table(table_name)) storage_manager.drop_table(table_name);
    storage_manager.add_table(table_name, table_info.table);
    std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
  }

  metrics.store_duration = timer.lap();

  std::cout << "- Adding tables to StorageManager and generating statistics done ("
            << format_duration(metrics.store_duration) << ")" << std::endl;

  /**
   * Create indexes if requested by the user
   */
  if (_benchmark_config->indexes) {
    std::cout << "- Creating indexes" << std::endl;
    const auto& indexes_by_table = _indexes_by_table();
    if (indexes_by_table.empty()) {
      std::cout << "-  No indexes defined by benchmark" << std::endl;
    }
    for (const auto& [table_name, indexes] : indexes_by_table) {
      const auto& table = table_info_by_name[table_name].table;

      for (const auto& index_columns : indexes) {
        auto column_ids = std::vector<ColumnID>{};

        for (const auto& index_column : index_columns) {
          column_ids.emplace_back(table->column_id_by_name(index_column));
        }

        std::cout << "-  Creating index on " << table_name << " [ ";
        for (const auto& index_column : index_columns) {
          std::cout << index_column << " ";
        }
        std::cout << "] " << std::flush;
        Timer per_index_timer;

        if (column_ids.size() == 1) {
          table->create_index<GroupKeyIndex>(column_ids);
        } else {
          table->create_index<CompositeGroupKeyIndex>(column_ids);
        }

        std::cout << "(" << per_index_timer.lap_formatted() << ")" << std::endl;
      }
    }
    metrics.index_duration = timer.lap();
    std::cout << "- Creating indexes done (" << format_duration(metrics.index_duration) << ")" << std::endl;
  } else {
    std::cout << "- No indexes created as --indexes was not specified or set to false" << std::endl;
  }
}

std::shared_ptr<BenchmarkConfig> AbstractTableGenerator::create_benchmark_config_with_chunk_size(
    ChunkOffset chunk_size) {
  auto config = BenchmarkConfig::get_default_config();
  config.chunk_size = chunk_size;
  return std::make_shared<BenchmarkConfig>(config);
}

AbstractTableGenerator::IndexesByTable AbstractTableGenerator::_indexes_by_table() const { return {}; }

AbstractTableGenerator::SortOrderByTable AbstractTableGenerator::_sort_order_by_table() const { return {}; }

void AbstractTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {}

}  // namespace opossum
