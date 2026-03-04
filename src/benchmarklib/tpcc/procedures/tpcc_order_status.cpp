#include "tpcc_order_status.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <memory>
#include <random>
#include <tuple>

#include "benchmark_sql_executor.hpp"
#include "storage/table.hpp"
#include "tpcc/procedures/abstract_tpcc_procedure.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TPCCOrderStatus::TPCCOrderStatus(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor) {
  auto warehouse_dist = std::uniform_int_distribution<>{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  // There are always exactly 9 districts per warehouse
  auto district_dist = std::uniform_int_distribution<>{1, 10};
  d_id = district_dist(_random_engine);

  // Select 6 out of 10 customers by last name
  auto customer_selection_method_dist = std::uniform_int_distribution<>{1, 10};
  select_customer_by_name = customer_selection_method_dist(_random_engine) <= 6;
  if (select_customer_by_name) {
    customer = pmr_string{_tpcc_random_generator.last_name(_tpcc_random_generator.nurand(255, 0, 999))};
  } else {
    customer = static_cast<int32_t>(_tpcc_random_generator.nurand(1023, 1, 3000));
  }
}

bool TPCCOrderStatus::_on_execute() {
  auto customer_table = std::shared_ptr<const Table>{};
  auto customer_id = int32_t{};

  if (!select_customer_by_name) {
    // Case 1 - Select customer by ID
    std::tie(std::ignore, customer_table) = _sql_executor.execute(
        std::format("EXECUTE order_status_select_customer_by_id({}, {}, {})", w_id, d_id, std::get<int32_t>(customer)));
    Assert(customer_table && customer_table->row_count() == 1, "Did not find customer by ID (or found more than one).");

    customer_id = std::get<int32_t>(customer);
  } else {
    // Case 2 - Select customer by name
    std::tie(std::ignore, customer_table) = _sql_executor.execute(std::format(
        "EXECUTE order_status_select_customer_by_name({}, {}, '{}')", w_id, d_id, std::get<pmr_string>(customer)));
    Assert(customer_table->row_count() >= 1, "Did not find customer by name.");

    // Calculate ceil(n/2)
    const auto customer_offset =
        static_cast<size_t>(std::min(std::ceil(static_cast<double>(customer_table->row_count()) / 2.0),
                                     static_cast<double>(customer_table->row_count() - 1)));
    customer_id = *customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);
  }

  // Retrieve order
  const auto order_select_pair =
      _sql_executor.execute(std::format("EXECUTE order_status_retrieve_order({}, {}, {})", w_id, d_id, customer_id));
  const auto& order_table = order_select_pair.second;
  // Returns multiple orders, we are interested in the latest one
  Assert(order_table && order_table->row_count() >= 1, "Unexpected state of ORDER table.");
  o_id = *order_table->get_value<int32_t>(ColumnID{0}, 0);
  o_entry_d = *order_table->get_value<int32_t>(ColumnID{1}, 0);
  o_carrier_id = order_table->get_value<int32_t>(ColumnID{2}, 0);

  // Retrieve order lines
  const auto order_line_select_pair =
      _sql_executor.execute(std::format("EXECUTE order_status_retrieve_order_lines({}, {}, {})", w_id, d_id, o_id));
  const auto& order_line_table = order_line_select_pair.second;
  const auto order_line_count = order_line_table->row_count();
  Assert(order_line_table && order_line_count >= 5 && order_line_count <= 15, "Unexpected state of ORDER_LINE table.");
  for (auto row = size_t{0}; row < order_line_count; ++row) {
    ol_quantity_sum += *order_line_table->get_value<int32_t>(ColumnID{2}, row);
  }

  _sql_executor.commit();
  return true;
}

}  // namespace hyrise
