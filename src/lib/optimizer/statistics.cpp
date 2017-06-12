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
  auto row_count = table->row_count();
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto aggregate = std::make_shared<Aggregate>(
      table_wrapper,
      std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("count"), Count)},
      std::vector<std::string>{column_name});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  auto distinct_count = aggregate_table->get_value<double>(0, 0);

  return row_count / distinct_count;
}

}  // namespace opossum
