#include "sqlite_testrunner.hpp"

#include "constant_mappings.hpp"

namespace opossum {

void SQLiteTestRunner::SetUpTestCase() {
  _sqlite = std::make_unique<SQLiteWrapper>();

  auto unencoded_table_cache = TableCache{};

  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner.tables");
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

    // Store loaded tables in a map that basically caches the loaded tables. In case the table
    // needs to be reloaded (e.g., due to modifications), we also store the file path.
    unencoded_table_cache.emplace(table_name, TableCacheEntry{load_table(table_file, CHUNK_SIZE), table_file});

    // Create test table and also table copy which is later used as the master to copy from.
    _sqlite->create_table_from_tbl(table_file, table_name);
    _sqlite->create_table_from_tbl(table_file, table_name + _master_table_suffix);
  }

  _table_cache_per_encoding.emplace(EncodingType::Unencoded, unencoded_table_cache);

  opossum::Topology::use_numa_topology();
  opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());
}

void SQLiteTestRunner::SetUp() {
  const auto& param = GetParam();

  const auto encoding_type = std::get<2>(param);

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

  // For proper testing, we reset the storage manager before EVERY test.
  StorageManager::reset();

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

      StorageManager::get().add_table(table_name, reloaded_table);
      table_cache.emplace(table_name, TableCacheEntry{reloaded_table, table_cache_entry.filename});

      // When tables in Hyrise have (potentially) modified, the should might be true for SQLite.
      _sqlite->reset_table_from_copy(table_name, table_name + _master_table_suffix);
    } else {
      StorageManager::get().add_table(table_name, table_cache_entry.table);
    }
  }

  SQLPhysicalPlanCache::get().clear();
}

std::vector<std::string> SQLiteTestRunner::queries() {
  static std::vector<std::string> queries;

  if (!queries.empty()) return queries;

  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  std::string query;

  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") continue;
    queries.emplace_back(std::move(query));
  }

  return queries;
}

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  const auto& param = GetParam();

  const auto& sql = std::get<0>(param);
  const auto use_jit = std::get<1>(param);
  const auto encoding_type = std::get<2>(param);

  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    lqp_translator = std::make_shared<JitAwareLQPTranslator>();
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }

  SCOPED_TRACE("Query '" + sql + "'" + (use_jit ? " with JIT" : " without JIT") + " with encoding " +
               encoding_type_to_string.left.at(encoding_type));

  auto sql_pipeline = SQLPipelineBuilder{sql}.with_lqp_translator(lqp_translator).create_pipeline();

  const auto result_table = sql_pipeline.get_result_table();

  for (const auto& plan : sql_pipeline.get_optimized_logical_plans()) {
    for (const auto& table_name : lqp_find_modified_tables(plan)) {
      // mark table cache entry as dirty, when table has been modified
      _table_cache_per_encoding.at(encoding_type).at(table_name).dirty = true;
    }
  }

  auto sqlite_result_table = _sqlite->execute_query(sql);

  // The problem is that we can only infer column types from sqlite if they have at least one row.
  ASSERT_TRUE(result_table && result_table->row_count() > 0 && sqlite_result_table &&
              sqlite_result_table->row_count() > 0)
      << "The SQLiteTestRunner cannot handle queries without results";

  auto order_sensitivity = OrderSensitivity::No;

  const auto& parse_result = sql_pipeline.get_parsed_sql_statements().back();
  if (parse_result->getStatements().front()->is(hsql::kStmtSelect)) {
    auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result->getStatements().back());
    if (select_statement->order != nullptr) {
      order_sensitivity = OrderSensitivity::Yes;
    }
  }

  ASSERT_TRUE(check_table_equal(result_table, sqlite_result_table, order_sensitivity, TypeCmpMode::Lenient,
                                FloatComparisonMode::RelativeDifference))
      << "Query failed: " << sql;

  // Delete newly created views in sqlite
  for (const auto& plan : sql_pipeline.get_optimized_logical_plans()) {
    if (const auto create_view = std::dynamic_pointer_cast<CreateViewNode>(plan)) {
      _sqlite->execute_query("DROP VIEW IF EXISTS " + create_view->view_name() + ";");
    }
  }
}

}  // namespace opossum
