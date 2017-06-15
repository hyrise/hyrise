#include "statistics.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/table_statistics.hpp"

namespace opossum {

std::shared_ptr<TableStatistics> Statistics::predicate_stats(const std::shared_ptr<TableStatistics> table_statistics,
                                                        const std::string &column_name, const std::string &op,
                                                        const AllParameterVariant value,
                                                        const optional<AllTypeVariant> value2) {
  // currently assuming all values are equally distributed
  auto row_count = table_statistics->row_count();
  if (row_count == 0) {
    return table_statistics->shared_clone(0.);
  }

  // TODO(mp): extend for other comparison operators
  if (op == "=") {
    auto distinct_count = table_statistics->get_column_statistics(column_name)->get_distinct_count();
    return table_statistics->shared_clone(row_count / static_cast<double>(distinct_count));
  }  // else if (op == "!=") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "<") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "<=") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == ">") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == ">=") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "BETWEEN") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "LIKE") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else {
  //   Fail(std::string("unknown operator ") + op);
  // }

  auto distinct_count = table_statistics->get_column_statistics(column_name)->get_distinct_count();
  // Brace yourselves.
  return table_statistics->shared_clone(row_count / static_cast<double>(distinct_count));
}

}  // namespace opossum
