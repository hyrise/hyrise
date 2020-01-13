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

#include "base_test.hpp"

#include "SQLParser.h"

#include "concurrency/transaction_context.hpp"
#include "constant_mappings.hpp"
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

using SQLiteTestRunnerParam = std::tuple<std::pair<size_t /* line */, std::string /* sql */>, EncodingType>;

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

  // Returns pair of the line in the sql file and the query itself
  static std::vector<std::pair<size_t, std::string>> queries();

  inline static std::unique_ptr<SQLiteWrapper> _sqlite;
  inline static std::map<EncodingType, TableCache> _table_cache_per_encoding;
  inline static std::string _master_table_suffix = "_master_copy";
};

auto sqlite_testrunner_formatter = [](const ::testing::TestParamInfo<SQLiteTestRunnerParam>& info) {
  const auto& query_pair = std::get<0>(info.param);
  const auto& encoding_type = std::get<1>(info.param);

  return std::string{"Line"} + std::to_string(query_pair.first) + "With" +
         encoding_type_to_string.left.at(encoding_type);
};

}  // namespace opossum
