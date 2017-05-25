#include "order_status.hpp"

#include <math.h>

#include "../../lib/types.hpp"

using namespace opossum;

namespace tpcc {

OrderStatusResult AbstractOrderStatusQueryImpl::run_transaction(const OrderStatusParams &params) {
  OrderStatusResult result;

  std::vector<AllTypeVariant> customer;

  if (params.order_status_by == OrderStatusBy::CustomerLastName) {
    auto get_customer_tasks = get_customer_by_name(params.c_last, params.c_d_id, params.c_w_id);
    schedule_tasks_and_wait(get_customer_tasks);

    auto num_names = get_customer_tasks.back()->get_operator()->get_output()->row_count();
    assert(num_names > 0);

    customer = get_from_table_at_row(get_customer_tasks.back()->get_operator()->get_output(), ceil(num_names / 2));
  } else {
    auto get_customer_tasks = get_customer_by_id(params.c_id, params.c_d_id, params.c_w_id);
    schedule_tasks_and_wait(get_customer_tasks);

    assert(get_customer_tasks.back()->get_operator()->get_output()->row_count() == 1);
    customer = get_from_table_at_row(get_customer_tasks.back()->get_operator()->get_output(), 0);
  }

  result.c_id = customer[0];
  result.c_first = customer[1];
  result.c_middle = customer[2];
  result.c_last = customer[3];
  result.c_balance = customer[4];

  auto get_order_tasks = get_orders();
  schedule_tasks_and_wait(get_order_tasks);

  auto order = get_from_table_at_row(get_order_tasks.back()->get_operator()->get_output(), 0);

  result.o_id = order[0];
  result.o_carrier_id = order[1];
  result.o_entry_d = order[2];

  auto get_order_line_tasks = get_order_lines(0, params.c_d_id, params.c_w_id);
  schedule_tasks_and_wait(get_order_line_tasks);

  auto order_lines_table = get_order_line_tasks.back()->get

  return result;
}

void AbstractOrderStatusQueryImpl::run_and_test_transaction_from_json(const nlohmann::json &json_params,
                                                                      const nlohmann::json &json_results) {
  OrderStatusParams params;
  params.c_w_id = json_params["c_w_id"];
  params.c_d_id = json_params["c_d_id"];

  if (json_params["case"] == 1) {
    params.order_status_by = OrderStatusBy::CustomerNumber;
    params.c_id = json_params["c_id"];
  } else {
    params.order_status_by = OrderStatusBy::CustomerLastName;
    params.c_last = json_params["c_last"];
  }

  auto result = run_transaction(params);


}

}