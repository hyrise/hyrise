#include "order_status.hpp"

#include <math.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "helper.hpp"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/table.hpp"

#include "types.hpp"

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
  OrderStatusResult result;

  opossum::TransactionManager::get().run_transaction([&](std::shared_ptr<opossum::TransactionContext> t_context) {
    if (params.order_status_by == OrderStatusBy::CustomerLastName) {
      auto get_customer_tasks = get_customer_by_name(params.c_last, params.c_d_id, params.c_w_id);
      execute_tasks_with_context(get_customer_tasks, t_context);

      const auto customers_table = get_customer_tasks.back()->get_operator()->get_output();
      const auto num_names = customers_table->row_count();
      DebugAssert(num_names > 0, "No such customer named '" + params.c_last + "'");

      const auto row = static_cast<size_t>(ceil(num_names - 1) / 2);

      result.c_balance = customers_table->get_value<float>(opossum::ColumnID(0), row);
      result.c_first = customers_table->get_value<std::string>(opossum::ColumnID(1), row);
      result.c_middle = customers_table->get_value<std::string>(opossum::ColumnID(2), row);
      result.c_last = params.c_last;
      result.c_id = customers_table->get_value<int32_t>(opossum::ColumnID(3), row);
    } else {
      auto get_customer_tasks = get_customer_by_id(params.c_id, params.c_d_id, params.c_w_id);
      execute_tasks_with_context(get_customer_tasks, t_context);
      DebugAssert(get_customer_tasks.back()->get_operator()->get_output()->row_count() == 1,
                  "Selecting by ID has to yield exactly one customer");

      const auto customers_table = get_customer_tasks.back()->get_operator()->get_output();

      result.c_balance = customers_table->get_value<float>(opossum::ColumnID(0), 0);
      result.c_first = customers_table->get_value<std::string>(opossum::ColumnID(1), 0);
      result.c_middle = customers_table->get_value<std::string>(opossum::ColumnID(2), 0);
      result.c_last = customers_table->get_value<std::string>(opossum::ColumnID(3), 0);
      result.c_id = params.c_id;
    }

    auto get_order_tasks = get_orders(result.c_id, params.c_d_id, params.c_w_id);
    execute_tasks_with_context(get_order_tasks, t_context);

    const auto orders_table = get_order_tasks.back()->get_operator()->get_output();

    result.o_id = orders_table->get_value<int32_t>(opossum::ColumnID(0), 0);
    result.o_carrier_id = orders_table->get_value<int32_t>(opossum::ColumnID(1), 0);
    result.o_entry_d = orders_table->get_value<int32_t>(opossum::ColumnID(2), 0);

    auto get_order_line_tasks = get_order_lines(result.o_id, params.c_d_id, params.c_w_id);
    execute_tasks_with_context(get_order_line_tasks, t_context);

    auto order_lines_table = get_order_line_tasks.back()->get_operator()->get_output();

    result.order_lines.reserve(order_lines_table->row_count());
    for (uint32_t r = 0; r < order_lines_table->row_count(); r++) {
      OrderStatusOrderLine order_line;

      order_line.ol_i_id = order_lines_table->get_value<int32_t>(opossum::ColumnID(0), r);
      order_line.ol_supply_w_id = order_lines_table->get_value<int32_t>(opossum::ColumnID(1), r);
      order_line.ol_quantity = order_lines_table->get_value<int32_t>(opossum::ColumnID(2), r);
      order_line.ol_amount = order_lines_table->get_value<float>(opossum::ColumnID(3), r);
      order_line.ol_delivery_d = order_lines_table->get_value<int32_t>(opossum::ColumnID(4), r);

      result.order_lines.emplace_back(order_line);
    }
  });

  return result;
}

TaskVector OrderStatusRefImpl::get_customer_by_name(const std::string c_last, const int c_d_id, const int c_w_id) {
  /**
   * SELECT c_balance, c_first, c_middle, c_id
   * FROM customer
   * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
   * ORDER BY c_first;
   */
  auto gt_customer = std::make_shared<opossum::GetTable>("CUSTOMER");
  auto validate = std::make_shared<opossum::Validate>(gt_customer);

  auto first_filter = std::make_shared<opossum::TableScan>(validate, opossum::ColumnID{5} /* "C_LAST" */,
                                                           opossum::ScanType::OpEquals, c_last);

  auto second_filter = std::make_shared<opossum::TableScan>(first_filter, opossum::ColumnID{1} /* "C_D_ID" */,
                                                            opossum::ScanType::OpEquals, c_d_id);

  auto third_filter = std::make_shared<opossum::TableScan>(second_filter, opossum::ColumnID{2} /* "C_W_ID" */,
                                                           opossum::ScanType::OpEquals, c_w_id);

  auto projection = std::make_shared<opossum::Projection>(
      third_filter, opossum::Projection::ColumnExpressions(
                        {opossum::Expression::create_column(opossum::ColumnID{16} /* "C_BALANCE" */),
                         opossum::Expression::create_column(opossum::ColumnID{3}) /* "C_FIRST" */,
                         opossum::Expression::create_column(opossum::ColumnID{4} /* "C_MIDDLE" */),
                         opossum::Expression::create_column(opossum::ColumnID{0} /* "C_ID" */)}));

  auto sort = std::make_shared<opossum::Sort>(projection, opossum::ColumnID{1} /* "C_FIRST" */,
                                              opossum::OrderByMode::Ascending);

  auto gt_customer_task = std::make_shared<opossum::OperatorTask>(gt_customer);
  auto validate_task = std::make_shared<opossum::OperatorTask>(validate);
  auto first_filter_task = std::make_shared<opossum::OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<opossum::OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<opossum::OperatorTask>(third_filter);
  auto projection_task = std::make_shared<opossum::OperatorTask>(projection);
  auto sort_task = std::make_shared<opossum::OperatorTask>(sort);

  gt_customer_task->set_as_predecessor_of(validate_task);
  validate_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);
  projection_task->set_as_predecessor_of(sort_task);

  return {gt_customer_task,  validate_task,   first_filter_task, second_filter_task,
          third_filter_task, projection_task, sort_task};
}

TaskVector OrderStatusRefImpl::get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) {
  /**
   * SQL SELECT c_balance, c_first, c_middle, c_last
   * FROM customer
   * WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
   */
  auto gt_customer = std::make_shared<opossum::GetTable>("CUSTOMER");
  auto validate = std::make_shared<opossum::Validate>(gt_customer);

  auto first_filter = std::make_shared<opossum::TableScan>(validate, opossum::ColumnID{0} /* "C_ID" */,
                                                           opossum::ScanType::OpEquals, c_id);

  auto second_filter = std::make_shared<opossum::TableScan>(first_filter, opossum::ColumnID{1} /* "C_D_ID" */,
                                                            opossum::ScanType::OpEquals, c_d_id);

  auto third_filter = std::make_shared<opossum::TableScan>(second_filter, opossum::ColumnID{2} /* "C_W_ID" */,
                                                           opossum::ScanType::OpEquals, c_w_id);

  auto projection = std::make_shared<opossum::Projection>(
      third_filter, opossum::Projection::ColumnExpressions(
                        {opossum::Expression::create_column(opossum::ColumnID{16} /* "C_BALANCE" */),
                         opossum::Expression::create_column(opossum::ColumnID{3} /* "C_FIRST" */),
                         opossum::Expression::create_column(opossum::ColumnID{4} /* "C_MIDDLE" */),
                         opossum::Expression::create_column(opossum::ColumnID{5} /* "C_LAST" */)}));

  auto gt_customer_task = std::make_shared<opossum::OperatorTask>(gt_customer);
  auto validate_task = std::make_shared<opossum::OperatorTask>(validate);
  auto first_filter_task = std::make_shared<opossum::OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<opossum::OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<opossum::OperatorTask>(third_filter);
  auto projection_task = std::make_shared<opossum::OperatorTask>(projection);

  gt_customer_task->set_as_predecessor_of(validate_task);
  validate_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);

  return {gt_customer_task, validate_task, first_filter_task, second_filter_task, third_filter_task, projection_task};
}

TaskVector OrderStatusRefImpl::get_orders(const int o_c_id, const int o_d_id, const int o_w_id) {
  /**
   * SELECT o_id, o_carrier_id, o_entry_d
   * FROM orders
   * WHERE o_c_id=o_c_id AND o_d_id=o_d_id AND o_w_id=o_w_id
   * ORDER BY o_id DESC
   * LIMIT 1;
   */
  auto gt_orders = std::make_shared<opossum::GetTable>("ORDER");
  auto validate = std::make_shared<opossum::Validate>(gt_orders);

  auto first_filter = std::make_shared<opossum::TableScan>(validate, opossum::ColumnID{3} /* "O_C_ID" */,
                                                           opossum::ScanType::OpEquals, o_c_id);

  auto second_filter = std::make_shared<opossum::TableScan>(first_filter, opossum::ColumnID{1} /* "O_D_ID" */,
                                                            opossum::ScanType::OpEquals, o_d_id);

  auto third_filter = std::make_shared<opossum::TableScan>(second_filter, opossum::ColumnID{2} /* "O_W_ID" */,
                                                           opossum::ScanType::OpEquals, o_w_id);

  // "O_ID", "O_CARRIER_ID", "O_ENTRY_D"
  auto projection = std::make_shared<opossum::Projection>(
      third_filter, opossum::Projection::ColumnExpressions({opossum::Expression::create_column(opossum::ColumnID{0}),
                                                            opossum::Expression::create_column(opossum::ColumnID{5}),
                                                            opossum::Expression::create_column(opossum::ColumnID{4})}));

  auto sort =
      std::make_shared<opossum::Sort>(projection, opossum::ColumnID{0} /* "O_ID" */, opossum::OrderByMode::Descending);
  auto limit = std::make_shared<opossum::Limit>(sort, 1);

  auto gt_orders_task = std::make_shared<opossum::OperatorTask>(gt_orders);
  auto validate_task = std::make_shared<opossum::OperatorTask>(validate);
  auto first_filter_task = std::make_shared<opossum::OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<opossum::OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<opossum::OperatorTask>(third_filter);
  auto projection_task = std::make_shared<opossum::OperatorTask>(projection);
  auto sort_task = std::make_shared<opossum::OperatorTask>(sort);
  auto limit_task = std::make_shared<opossum::OperatorTask>(limit);

  gt_orders_task->set_as_predecessor_of(validate_task);
  validate_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);
  projection_task->set_as_predecessor_of(sort_task);
  sort_task->set_as_predecessor_of(limit_task);

  return {gt_orders_task,    validate_task,   first_filter_task, second_filter_task,
          third_filter_task, projection_task, sort_task,         limit_task};
}

TaskVector OrderStatusRefImpl::get_order_lines(const int o_id, const int d_id, const int w_id) {
  /**
   * SELECT ol_i_id, ol_supply_w_id, ol_quantity,
   * ol_amount, ol_delivery_d
   * FROM order_line
   * WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
   */
  auto gt_order_lines = std::make_shared<opossum::GetTable>("ORDER-LINE");
  auto validate = std::make_shared<opossum::Validate>(gt_order_lines);

  auto first_filter = std::make_shared<opossum::TableScan>(validate, opossum::ColumnID{0} /* "OL_O_ID" */,
                                                           opossum::ScanType::OpEquals, o_id);

  auto second_filter = std::make_shared<opossum::TableScan>(first_filter, opossum::ColumnID{1} /* "OL_D_ID" */,
                                                            opossum::ScanType::OpEquals, d_id);

  auto third_filter = std::make_shared<opossum::TableScan>(second_filter, opossum::ColumnID{2} /* "OL_W_ID" */,
                                                           opossum::ScanType::OpEquals, w_id);

  auto projection = std::make_shared<opossum::Projection>(
      third_filter, opossum::Projection::ColumnExpressions(
                        {opossum::Expression::create_column(opossum::ColumnID{4} /* "OL_I_ID" */),
                         opossum::Expression::create_column(opossum::ColumnID{5} /* "OL_SUPPLY_W_ID" */),
                         opossum::Expression::create_column(opossum::ColumnID{7} /* "OL_QUANTITY" */),
                         opossum::Expression::create_column(opossum::ColumnID{8} /* "OL_AMOUNT" */),
                         opossum::Expression::create_column(opossum::ColumnID{6} /* "OL_DELIVERY_D" */)}));

  auto gt_order_lines_task = std::make_shared<opossum::OperatorTask>(gt_order_lines);
  auto validate_task = std::make_shared<opossum::OperatorTask>(validate);
  auto first_filter_task = std::make_shared<opossum::OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<opossum::OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<opossum::OperatorTask>(third_filter);
  auto projection_task = std::make_shared<opossum::OperatorTask>(projection);

  gt_order_lines_task->set_as_predecessor_of(validate_task);
  validate_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(projection_task);

  return {gt_order_lines_task, validate_task,     first_filter_task,
          second_filter_task,  third_filter_task, projection_task};
}

}  // namespace tpcc

namespace nlohmann {

void adl_serializer<tpcc::OrderStatusParams>::to_json(nlohmann::json &j, const tpcc::OrderStatusParams &v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::OrderStatusParams>::from_json(const nlohmann::json &j, tpcc::OrderStatusParams &v) {
  v.c_w_id = j["c_w_id"];
  v.c_d_id = j["c_d_id"];

  if (j["case"] == 1) {
    v.order_status_by = tpcc::OrderStatusBy::CustomerNumber;
    v.c_id = j["c_id"];
  } else {
    v.order_status_by = tpcc::OrderStatusBy::CustomerLastName;
    v.c_last = j["c_last"];
  }
}

void adl_serializer<tpcc::OrderStatusOrderLine>::to_json(nlohmann::json &j, const tpcc::OrderStatusOrderLine &v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::OrderStatusOrderLine>::from_json(const nlohmann::json &j, tpcc::OrderStatusOrderLine &v) {
  v.ol_supply_w_id = j["ol_supply_w_id"];
  v.ol_i_id = j["ol_i_id"];
  v.ol_quantity = j["ol_quantity"];
  v.ol_amount = j["ol_amount"];
  v.ol_delivery_d = j["ol_delivery_d"];
}

void adl_serializer<tpcc::OrderStatusResult>::to_json(nlohmann::json &j, const tpcc::OrderStatusResult &v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::OrderStatusResult>::from_json(const nlohmann::json &j, tpcc::OrderStatusResult &v) {
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
      tpcc::OrderStatusOrderLine ol = j_ol;
      v.order_lines.emplace_back(ol);
    }
  }
}
}  // namespace nlohmann
