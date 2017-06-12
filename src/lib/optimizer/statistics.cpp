#include "statistics.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

size_t Statistics::predicate_result_size(const std::shared_ptr<Table> table, const std::string &column_name,
                                         const std::string &op, const AllParameterVariant value,
                                         const optional<AllTypeVariant> value2) {
  // currently assuming all values are equally distributed
  auto row_count = table->row_count();
  if (row_count == 0) {
    return 0;
  }

  if (op == "=") {
    auto distinct_count = Statistics::get_distinct_count(table, column_name);
    return row_count / distinct_count;
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
  return 0;
}

size_t Statistics::get_distinct_count(const std::shared_ptr<Table> table, const std::string &column_name) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto aggregate = std::make_shared<Aggregate>(
      table_wrapper,
      std::vector<std::pair<std::string, AggregateFunction>>{},
      std::vector<std::string>{column_name});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  return aggregate_table->row_count();
}

}  // namespace opossum
