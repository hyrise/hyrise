#pragma once

#include <memory>
#include <string>

#include "common.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

class SQLQueryPlanVisualizer {
 public:
  static const std::string png_filename;

  static void visualize(const SQLQueryPlan &plan);

 protected:
  static void _visualize_subtree(const std::shared_ptr<const AbstractOperator> &op, std::ofstream &file);
  static void _visualize_dataflow(const std::shared_ptr<const AbstractOperator> &from,
                                  const std::shared_ptr<const AbstractOperator> &to, std::ofstream &file);
};

}  // namespace opossum
