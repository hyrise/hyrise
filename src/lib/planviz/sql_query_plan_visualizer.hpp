#pragma once

#include <memory>
#include <string>

#include "common.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

class SQLQueryPlanVisualizer {
 public:
  static const std::string png_filename;

  static void visualize(SQLQueryPlan &plan);

 protected:
  static void _visualize_subtree(const std::shared_ptr<const AbstractOperator> &op, std::ofstream &file);
};

}  // namespace opossum
