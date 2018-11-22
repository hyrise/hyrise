#pragma once

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"

namespace opossum {

struct SQLiteTestRunnerTestCase {
  std::string sql;
  bool use_jit{false};
  EncodingType encoding_type{EncodingType::Unencoded};
};

class SQLiteTestRunner : public BaseTestWithParam<SQLiteTestRunnerTestCase> {
 public:
  static constexpr ChunkOffset CHUNK_SIZE = 10;

  // Structure to cache initially loaded tables and store their file paths
  // to reload the the table from the given tbl file whenever required.
  struct TableCacheEntry {
    std::shared_ptr<Table> table;
    std::string filename;
    ChunkEncodingSpec chunk_encoding_spec{};
    bool dirty{false};
  };

  using TableCache = std::map<std::string, TableCacheEntry>;

  static void SetUpTestCase() {  // called ONCE before the tests
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

  void SetUp() override {
    const auto& param = GetParam();

    auto table_cache_iter = _table_cache_per_encoding.find(param.encoding_type);

    if (table_cache_iter == _table_cache_per_encoding.end()) {
      const auto& unencoded_table_cache = _table_cache_per_encoding.at(EncodingType::Unencoded);
      auto encoded_table_cache = TableCache{};

      for (auto const& [table_name, table_cache_entry] : unencoded_table_cache) {
        auto table = load_table(table_cache_entry.filename, CHUNK_SIZE);

        auto chunk_encoding_spec = ChunkEncodingSpec{table->column_count(), EncodingType::Unencoded};
        for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
          if (encoding_supports_data_type(param.encoding_type, table->column_data_type(column_id))) {
            chunk_encoding_spec[column_id] = param.encoding_type;
          }
        }

        ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
        encoded_table_cache.emplace(table_name, TableCacheEntry{table, table_cache_entry.filename, chunk_encoding_spec, false});
      }

      table_cache_iter = _table_cache_per_encoding.emplace(param.encoding_type, encoded_table_cache).first;
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
        if (param.encoding_type != EncodingType::Unencoded) {
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

  static std::vector<std::string> queries()  {
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

  inline static std::unique_ptr<SQLiteWrapper> _sqlite;
  inline static std::map<EncodingType, TableCache> _table_cache_per_encoding;
  inline static std::string _master_table_suffix = "_master_copy";
};

}  // namespace opossum
