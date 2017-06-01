#include "order_status.hpp"

#include <math.h>
#include <sstream>

#include "../../lib/types.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/operators/limit.hpp"

#define VERBOSE 1

using namespace opossum;

namespace tpcc {

std::string OrderStatusParams::to_string() const {
  std::stringstream s;

  s << "{c_w_id: " << c_w_id << "; c_d_id: " << c_d_id << "; ";
  if (order_status_by == OrderStatusBy::CustomerLastName) {
    s << "c_last: " << c_last;
  } else {
    s << "c_id: " << c_id;
  }

  s << "}" << std::endl;
  return s.str();
}

OrderStatusResult AbstractOrderStatusImpl::run_transaction(const OrderStatusParams &params) {
#if VERBOSE
  std::cout << "OrderStatus: Running transaction: " << params.to_string() << std::endl;
#endif

  OrderStatusResult result;

  if (params.order_status_by == OrderStatusBy::CustomerLastName) {
    auto get_customer_tasks = get_customer_by_name(params.c_last, params.c_d_id, params.c_w_id);
    AbstractScheduler::schedule_tasks_and_wait(get_customer_tasks);

#if VERBOSE
    std::cout << "OrderStatus: GetCustomerByLastName:" << std::endl;
    Print(get_customer_tasks.back()->get_operator()).execute();
#endif

    auto num_names = get_customer_tasks.back()->get_operator()->get_output()->row_count();
    assert(num_names > 0);

    auto customer = get_customer_tasks.back()->get_operator()->get_output()->fetch_row(ceil(num_names / 2));

    result.c_balance = boost::get<float>(customer[0]);
    result.c_first = boost::get<std::string>(customer[1]);
    result.c_middle = boost::get<std::string>(customer[2]);
    result.c_last = params.c_last;
    result.c_id = boost::get<int32_t>(customer[3]);
  } else {
    auto get_customer_tasks = get_customer_by_id(params.c_id, params.c_d_id, params.c_w_id);
    AbstractScheduler::schedule_tasks_and_wait(get_customer_tasks);

    assert(get_customer_tasks.back()->get_operator()->get_output()->row_count() == 1);
    auto customer = get_customer_tasks.back()->get_operator()->get_output()->fetch_row(0);

    result.c_balance = boost::get<float>(customer[0]);
    result.c_first = boost::get<std::string>(customer[1]);
    result.c_middle = boost::get<std::string>(customer[2]);
    result.c_last = boost::get<std::string>(customer[3]);
    result.c_id = params.c_id;
  }

  auto get_order_tasks = get_orders();
  AbstractScheduler::schedule_tasks_and_wait(get_order_tasks);

  auto order = get_order_tasks.back()->get_operator()->get_output()->fetch_row(0);

  result.o_id = boost::get<int32_t>(order[0]);
  result.o_carrier_id = boost::get<int32_t>(order[1]);
  result.o_entry_d = boost::get<int32_t>(order[2]);

  auto get_order_line_tasks = get_order_lines(0, params.c_d_id, params.c_w_id);
  AbstractScheduler::schedule_tasks_and_wait(get_order_line_tasks);

  auto order_lines_table = get_order_line_tasks.back()->get_operator()->get_output();

  result.order_lines.reserve(order_lines_table->row_count());
  for (uint32_t r = 0; r < order_lines_table->row_count(); r++) {
    OrderStatusOrderLine order_line;

    const auto row = order_lines_table->fetch_row(r);

    order_line.ol_i_id = boost::get<int32_t>(row[0]);
    order_line.ol_supply_w_id = boost::get<int32_t>(row[1]);
    order_line.ol_quantity = boost::get<int32_t>(row[2]);
    order_line.ol_amount = boost::get<float>(row[3]);
    order_line.ol_delivery_d = boost::get<int32_t>(row[4]);

    result.order_lines.emplace_back(order_line);
  }

#if VERBOSE
  std::cout << "OrderStatus: Finished transaction" << std::endl;
#endif


  return result;
}

std::vector<std::shared_ptr<opossum::OperatorTask>>
OrderStatusRefImpl::get_customer_by_name(const std::string c_last, const int c_d_id,
                     const int c_w_id) {
  /**
 * SELECT c_balance, c_first, c_middle, c_id
 * FROM customer
 * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
 * ORDER BY c_first;
 */
  auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
  auto first_filter = std::make_shared<TableScan>(gt_customer, "C_LAST", "=", c_last);
  auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
  auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
  std::vector<std::string> columns = {"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_ID"};
  auto projection = std::make_shared<Projection>(third_filter, columns);
  auto sort = std::make_shared<Sort>(projection, "C_FIRST", true);

  auto gt_customer_task = std::make_shared<OperatorTask>(gt_customer);
  auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
  auto projection_task = std::make_shared<OperatorTask>(projection);
  auto sort_task = std::make_shared<OperatorTask>(sort);

  gt_customer_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);
  projection_task->set_as_predecessor_of(sort_task);

  return {gt_customer_task, first_filter_task, second_filter_task, third_filter_task, projection_task, sort_task};
}

std::vector<std::shared_ptr<opossum::OperatorTask>>
OrderStatusRefImpl::get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) {
  /**
   * SQL SELECT c_balance, c_first, c_middle, c_last
   * FROM customer
   * WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
   */
  auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
  auto first_filter = std::make_shared<TableScan>(gt_customer, "C_ID", "=", c_id);
  auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
  auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
  std::vector<std::string> columns = {"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_LAST"};
  auto projection = std::make_shared<Projection>(third_filter, columns);

  auto gt_customer_task = std::make_shared<OperatorTask>(gt_customer);
  auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
  auto projection_task = std::make_shared<OperatorTask>(projection);

  gt_customer_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);

  return {gt_customer_task, first_filter_task, second_filter_task, third_filter_task, projection_task};
}

std::vector<std::shared_ptr<opossum::OperatorTask>> OrderStatusRefImpl::get_orders() {
  /**
  * SELECT o_id, o_carrier_id, o_entry_d
  * FROM orders
  * ORDER BY o_id DESC
  * LIMIT 1;
  */
  auto gt_orders = std::make_shared<GetTable>("ORDER");
  std::vector<std::string> columns = {"O_ID", "O_CARRIER_ID", "O_ENTRY_D"};
  auto projection = std::make_shared<Projection>(gt_orders, columns);
  auto sort = std::make_shared<Sort>(projection, "O_ID", false);
  auto limit = std::make_shared<Limit>(sort, 1);

  auto gt_orders_task = std::make_shared<OperatorTask>(gt_orders);
  auto projection_task = std::make_shared<OperatorTask>(projection);
  auto sort_task = std::make_shared<OperatorTask>(sort);
  auto limit_task = std::make_shared<OperatorTask>(limit);

  gt_orders_task->set_as_predecessor_of(projection_task);
  projection_task->set_as_predecessor_of(sort_task);
  sort_task->set_as_predecessor_of(limit_task);

  return {gt_orders_task, projection_task, sort_task, limit_task};
}

std::vector<std::shared_ptr<opossum::OperatorTask>>
OrderStatusRefImpl::get_order_lines(const int o_id, const int d_id, const int w_id) {
  /**
  * SELECT ol_i_id, ol_supply_w_id, ol_quantity,
  * ol_amount, ol_delivery_d
  * FROM order_line
  * WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
  */
  auto gt_order_lines = std::make_shared<GetTable>("ORDER-LINE");
  auto first_filter = std::make_shared<TableScan>(gt_order_lines, "OL_O_ID", "=", o_id);
  auto second_filter = std::make_shared<TableScan>(first_filter, "OL_D_ID", "=", d_id);
  auto third_filter = std::make_shared<TableScan>(second_filter, "OL_W_ID", "=", w_id);
  std::vector<std::string> columns = {"OL_I_ID", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D"};
  auto projection = std::make_shared<Projection>(third_filter, columns);

  auto gt_order_lines_task = std::make_shared<OperatorTask>(gt_order_lines);
  auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
  auto projection_task = std::make_shared<OperatorTask>(projection);

  gt_order_lines_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);

  return {gt_order_lines_task, first_filter_task, second_filter_task, third_filter_task, projection_task};
}

}

namespace nlohmann {

using namespace opossum;
using namespace tpcc;

void adl_serializer<OrderStatusParams>::to_json(nlohmann::json &j, const OrderStatusParams &v) {
  throw "Not implemented";
}

void adl_serializer<OrderStatusParams>::from_json(const nlohmann::json &j, OrderStatusParams &v) {
  v.c_w_id = j["c_w_id"];
  v.c_d_id = j["c_d_id"];

  if (j["case"] == 1) {
    v.order_status_by = OrderStatusBy::CustomerNumber;
    v.c_id = j["c_id"];
  } else {
    v.order_status_by = OrderStatusBy::CustomerLastName;
    v.c_last = j["c_last"];
  }
}

void adl_serializer<OrderStatusOrderLine>::to_json(nlohmann::json &j, const OrderStatusOrderLine &v) {
  throw "Not implemented";
}

void adl_serializer<OrderStatusOrderLine>::from_json(const nlohmann::json &j, OrderStatusOrderLine &v) {
  v.ol_supply_w_id = j["ol_supply_w_id"];
  v.ol_i_id = j["ol_i_id"];
  v.ol_quantity = j["ol_quantity"];
  v.ol_amount = j["ol_amount"];
  v.ol_delivery_d = j["ol_delivery_d"];
}

void adl_serializer<OrderStatusResult>::to_json(nlohmann::json &j, const OrderStatusResult &v) {
  throw "Not implemented";
}

void adl_serializer<OrderStatusResult>::from_json(const nlohmann::json &j, OrderStatusResult &v) {
  v.c_id = j["c_id"];
  v.c_first = j["c_first"];
  v.c_middle = j["c_middle"];
  v.c_last = j["c_last"];
  v.c_balance = j["c_balance"];
  v.o_id = j["o_id"];
  v.o_carrier_id = j["o_carrier_id"];
  v.o_entry_d = j["o_entry_d"];

  const auto iter = j.find("order_lines");
  if (iter != j.end()) {
    for (const auto &j_ol : *iter) {
      OrderStatusOrderLine ol = j_ol;
      v.order_lines.emplace_back(ol);
    }
  }
}
}