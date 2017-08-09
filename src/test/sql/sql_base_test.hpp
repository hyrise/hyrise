#pragma once

#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class OperatorTask;

// The fixture for testing class GetTable.
class SQLBaseTest : public BaseTest {
 protected:
  void TearDown() override {}

  std::shared_ptr<OperatorTask> execute_query_task(const std::string& query, bool schedule_plan = false);

  void schedule_query_task(const std::string& query);

  size_t parse_tree_cache_hits;
  size_t query_plan_cache_hits;

  const std::string Q1 = "SELECT * FROM table_a;";
  const std::string Q2 = "SELECT * FROM table_b;";
  const std::string Q3 = "SELECT * FROM table_a WHERE a > 1;";
};

}  // namespace opossum
