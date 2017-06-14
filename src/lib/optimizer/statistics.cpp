#include "statistics.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/table_stats.hpp"

namespace opossum {

std::shared_ptr<TableStats> Statistics::predicate_stats(const std::shared_ptr<TableStats> table_stats,
                                                        const std::string &column_name, const std::string &op,
                                                        const AllParameterVariant value,
                                                        const optional<AllTypeVariant> value2) {
  // currently assuming all values are equally distributed
  auto row_count = table_stats->row_count();
  if (row_count == 0) {
    return table_stats->shared_clone(0);
  }

  if (op == "=") {
    auto distinct_count = table_stats->get_column_stats(column_name)->get_distinct_count();
    return table_stats->shared_clone(row_count / distinct_count);
  } else if (op == "!=") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else if (op == "<") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else if (op == "<=") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else if (op == ">") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else if (op == ">=") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else if (op == "BETWEEN") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else if (op == "LIKE") {
    Fail(std::string("operator not yet implemented: ") + op);
  } else {
    Fail(std::string("unknown operator ") + op);
  }
  return NULL;
}

}  // namespace opossum
