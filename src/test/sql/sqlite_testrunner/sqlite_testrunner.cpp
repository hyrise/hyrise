#include "sqlite_testrunner.hpp"

#include "constant_mappings.hpp"

namespace opossum {

void SQLiteTestRunner::SetUpTestCase() {
  /**
   * This loads the tables used for the SQLiteTestRunner into the Hyrise cache
   * (_table_cache_per_encoding[EncodingType::Unencoded]) and into SQLite.
   * Later, when running the individual queries, we only reload tables from disk if they have been modified by the
   * previous query.
   */

  _sqlite = std::make_unique<SQLiteWrapper>();

  auto unencoded_table_cache = TableCache{};

  std::ifstream file("resources/test_data/sqlite_testrunner.tables");
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }

    std::vector<std::string> args;
    boost::algorithm::split(args, line, boost::is_space());

    if (args.size() != 2) {
      continue;
    }

    std::string table_file = args.at(0);
    std::string table_name = args.at(1);

    const auto table = load_table(table_file, CHUNK_SIZE);

    // Store loaded tables in a map that basically caches the loaded tables. In case the table
    // needs to be reloaded (e.g., due to modifications), we also store the file path.
    unencoded_table_cache.emplace(table_name, TableCacheEntry{table, table_file});

    // Create test table and also table copy which is later used as the master to copy from.
    _sqlite->create_table(*table, table_name);
    _sqlite->create_table(*table, table_name + _master_table_suffix);
  }

  _table_cache_per_encoding.emplace(EncodingType::Unencoded, unencoded_table_cache);

  opossum::Hyrise::get().topology.use_numa_topology();
  opossum::Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
}

void SQLiteTestRunner::SetUp() {
  const auto& param = GetParam();

  /**
   * Encode Tables if no encoded variant of a Table is in the cache
   */
  const auto encoding_type = std::get<1>(param);
  auto table_cache_iter = _table_cache_per_encoding.find(encoding_type);

  if (table_cache_iter == _table_cache_per_encoding.end()) {
    const auto& unencoded_table_cache = _table_cache_per_encoding.at(EncodingType::Unencoded);
    auto encoded_table_cache = TableCache{};

    for (auto const& [table_name, table_cache_entry] : unencoded_table_cache) {
      auto table = load_table(table_cache_entry.filename, CHUNK_SIZE);

      auto chunk_encoding_spec = create_compatible_chunk_encoding_spec(*table, encoding_type);
      ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);

      encoded_table_cache.emplace(table_name,
                                  TableCacheEntry{table, table_cache_entry.filename, chunk_encoding_spec, false});
    }

    table_cache_iter = _table_cache_per_encoding.emplace(encoding_type, encoded_table_cache).first;
  }

  auto& table_cache = table_cache_iter->second;

  /**
   * Reset dirty tables in SQLite
   */
  for (const auto& table_name_and_cache : table_cache) {
    const auto& table_name = table_name_and_cache.first;

    // When tables in Hyrise were (potentially) modified, we assume the same happened in sqlite.
    // The SQLite table is considered dirty if any of its encoded versions in hyrise are dirty.
    const auto sqlite_table_dirty = std::any_of(
        _table_cache_per_encoding.begin(), _table_cache_per_encoding.end(), [&](const auto& table_cache_for_encoding) {
          const auto& table_cache_entry = table_cache_for_encoding.second.at(table_name);
          return table_cache_entry.dirty;
        });

    if (sqlite_table_dirty) {
      _sqlite->reset_table_from_copy(table_name, table_name + _master_table_suffix);
    }
  }

  /**
   * Populate the StorageManager with mint Tables with the correct encoding from the cache
   */
  for (auto const& [table_name, table_cache_entry] : table_cache) {
    /*
      Opossum:
        We start off with cached tables (SetUpTestCase) and add them to the resetted
        storage manager before each test here. In case tables have been modified, they are
        removed from the cache and we thus need to reload them from the initial tbl file.
      SQLite:
        Drop table and copy the whole table from the master table to reset all accessed tables.
    */
    if (table_cache_entry.dirty) {
      // 1. reload table from tbl file, 2. add table to storage manager, 3. cache table in map
      auto reloaded_table = load_table(table_cache_entry.filename, CHUNK_SIZE);
      if (encoding_type != EncodingType::Unencoded) {
        // Do not call ChunkEncoder when in Unencoded mode since the ChunkEncoder will also generate
        // pruning statistics and we want to run this test without them as well, so we hijack the Unencoded
        // mode for this.
        // TODO(anybody) Extract pruning statistics generation from ChunkEncoder, possibly as part of # 1153
        ChunkEncoder::encode_all_chunks(reloaded_table, table_cache_entry.chunk_encoding_spec);
      }

      Hyrise::get().storage_manager.add_table(table_name, reloaded_table);
      table_cache.emplace(table_name, TableCacheEntry{reloaded_table, table_cache_entry.filename});

    } else {
      Hyrise::get().storage_manager.add_table(table_name, table_cache_entry.table);
    }
  }
}

std::vector<std::string> SQLiteTestRunner::queries() {
  static std::vector<std::string> queries;

  if (!queries.empty()) return queries;

  std::ifstream file("resources/test_data/sqlite_testrunner_queries.sql");
  std::string query;

  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") continue;
    queries.emplace_back(std::move(query));
  }

  return queries;
}

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  const auto [sql, encoding_type] = GetParam();

  SCOPED_TRACE("Query '" + sql + "' with encoding " + encoding_type_to_string.left.at(encoding_type));

  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  // Execute query in Hyrise and SQLite
  const auto [pipeline_status, result_table] = sql_pipeline.get_result_table();
  ASSERT_EQ(pipeline_status, SQLPipelineStatus::Success);
  const auto sqlite_result_table = _sqlite->execute_query(sql);

  ASSERT_TRUE(result_table && result_table->row_count() > 0 && sqlite_result_table &&
              sqlite_result_table->row_count() > 0)
      << "The SQLiteTestRunner cannot handle queries without results. We can only infer column types from sqlite if "
         "they have at least one row";

  auto order_sensitivity = OrderSensitivity::No;
  const auto& parse_result = sql_pipeline.get_parsed_sql_statements().back();
  if (parse_result->getStatements().front()->is(hsql::kStmtSelect)) {
    auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result->getStatements().back());
    if (select_statement->order) {
      order_sensitivity = OrderSensitivity::Yes;
    }
  }

  const auto table_comparison_msg =
      check_table_equal(result_table, sqlite_result_table, order_sensitivity, TypeCmpMode::Lenient,
                        FloatComparisonMode::RelativeDifference, IgnoreNullable::Yes);

  if (table_comparison_msg) {
    FAIL() << "Query failed: " << *table_comparison_msg << std::endl;
  }

  // Mark Tables modified by the query as dirty
  for (const auto& plan : sql_pipeline.get_optimized_logical_plans()) {
    for (const auto& table_name : lqp_find_modified_tables(plan)) {
      // mark table cache entry as dirty, when table has been modified
      _table_cache_per_encoding.at(encoding_type).at(table_name).dirty = true;
    }
  }

  // Delete newly created views in sqlite
  for (const auto& plan : sql_pipeline.get_optimized_logical_plans()) {
    if (const auto create_view = std::dynamic_pointer_cast<CreateViewNode>(plan)) {
      _sqlite->execute_query("DROP VIEW IF EXISTS " + create_view->view_name + ";");
    }
  }
}

}  // namespace opossum
