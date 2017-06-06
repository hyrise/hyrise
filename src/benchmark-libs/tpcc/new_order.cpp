#include "new_order.hpp"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/scheduler/abstract_scheduler.hpp"

using namespace opossum;

namespace tpcc {

NewOrderResult AbstractNewOrderImpl::run_transaction(const NewOrderParams &params) {
  NewOrderResult result;

  auto t_context = TransactionManager::get().new_transaction_context();

  auto get_customer_and_warehouse_tax_rate_tasks = get_get_customer_and_warehouse_tax_rate_tasks(t_context, params.w_id,
                                                                                                 params.d_id,
                                                                                                 params.c_id);
  AbstractScheduler::schedule_tasks_and_wait(get_customer_and_warehouse_tax_rate_tasks);

  auto customer_and_warehouse_tax_rate = get_customer_and_warehouse_tax_rate_tasks.back()->get_operator()->get_output()->fetch_row(
    0);
  result.w_tax_rate = boost::get<float>(customer_and_warehouse_tax_rate[0]);
  result. = boost::get<float>(customer_and_warehouse_tax_rate[0]);

  auto get_district_tasks = get_get_district_tasks(t_context, params.d_id, params.w_id);
  AbstractScheduler::schedule_tasks_and_wait(get_district_tasks);

  //    auto output = last_get_district_task->get_operator()->get_output();

  auto increment_next_order_id_tasks = get_increment_next_order_id_tasks(t_context, params.d_id, params.w_id,
                                                                         params.next_o_id);
  AbstractScheduler::schedule_tasks_and_wait(increment_next_order_id_tasks);

  // TODO(tim): loop
  for (const auto &order_line_params : params.order_lines) {
    auto get_item_info_tasks = get_get_item_info_tasks(t_context, order_line_params.i_id);
    AbstractScheduler::schedule_tasks_and_wait(get_item_info_tasks);

    auto get_stock_info_tasks = get_get_stock_info_tasks(t_context, order_line_params.i_id, order_line_params.w_id);
    AbstractScheduler::schedule_tasks_and_wait(get_stock_info_tasks);

    auto update_stock_tasks = get_update_stock_tasks(t_context, s_quantity, order_line_params.i_id,
                                                     order_line_params.w_id);
    AbstractScheduler::schedule_tasks_and_wait(update_stock_tasks);
  }

  // Commit transaction.
  TransactionManager::get().prepare_commit(*t_context);

  auto commit = std::make_shared<CommitRecords>();
  commit->set_transaction_context(t_context);

  auto commit_task = std::make_shared<OperatorTask>(commit);
  commit_task->schedule();
  commit_task->join();

  TransactionManager::get().commit(*t_context);
}

TaskVector NewOrderRefImpl::get_get_customer_and_warehouse_tax_rate_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t w_id, const int32_t d_id,
  const int32_t c_id) {

}

TaskVector
NewOrderRefImpl::get_get_district_tasks(const std::shared_ptr<TransactionContext> t_context,
                                        const int32_t d_id, const int32_t w_id) {

}

TaskVector NewOrderRefImpl::get_increment_next_order_id_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t d_id, const int32_t d_w_id,
  const int32_t d_next_o_id) {

}

TaskVector NewOrderRefImpl::get_create_order_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t d_next_o_id, const int32_t d_id, const
int32_t w_id, const int32_t c_id, const int32_t o_entry_d, const int32_t o_carrier_id, const int32_t
  o_ol_cnt, const int32_t o_all_local) {

}

TaskVector NewOrderRefImpl::get_create_new_order_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t o_id, const int32_t d_id, const int32_t
w_id) {

}

TaskVector NewOrderRefImpl::get_get_item_info_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id) {

}

TaskVector NewOrderRefImpl::get_get_stock_info_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id, const int32_t ol_supply_w_id) {

}

TaskVector
NewOrderRefImpl::get_update_stock_tasks(const std::shared_ptr<TransactionContext> t_context,
                                        const int32_t s_quantity, const int32_t ol_i_id,
                                        const int32_t ol_supply_w_id) {

}
}