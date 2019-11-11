#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/print.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/sqlite_wrapper.hpp"

namespace opossum {

using SQLiteTestRunnerParam = std::tuple<std::string /* sql */, EncodingType>;

class SQLiteTestRunner : public BaseTestWithParam<SQLiteTestRunnerParam> {
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

  static void SetUpTestCase();

  void SetUp() override;

  static std::vector<std::string> queries();

  inline static std::unique_ptr<SQLiteWrapper> _sqlite;
  inline static std::map<EncodingType, TableCache> _table_cache_per_encoding;
  inline static std::string _master_table_suffix = "_master_copy";
};

}  // namespace opossum
