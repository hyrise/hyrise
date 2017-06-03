#include <concurrency/transaction_manager.hpp>
#include <concurrency/transaction_manager.hpp>
#include <memory>
#include <operators/commit_records.hpp>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include <operators/get_table.hpp>
#include <operators/limit.hpp>
#include <operators/projection.hpp>
#include <operators/sort.hpp>
#include <operators/table_scan.hpp>
#include <scheduler/operator_task.hpp>

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCPaymentBenchmark : public TPCCBenchmarkFixture {
 public:
  std::vector<std::shared_ptr<OperatorTask>> update_warehouse(const int w_id, const double payment_amount) {
    /**
     * UPDATE warehouse SET w_ytd = w_ytd + :h_amount
     * WHERE w_id=:w_id;
     */

    auto w_gt = std::make_shared<GetTable>("WAREHOUSE");
    auto w_ts = std::make_shared<TableScan>(w_gt, "W_ID", "=", w_id);
    auto projection = std::make_shared<Projection>(w_ts, std::vector<std::string>{"W_YTD"});

    return std::vector<std::shared_ptr<OperatorTask>>();
  }

  std::vector<std::shared_ptr<OperatorTask>> get_warehouse(const int w_id) {
    /**
     * SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
     * FROM warehouse
     * WHERE w_id=:w_id;
     */

    return std::vector<std::shared_ptr<OperatorTask>>();
  }

  std::vector<std::shared_ptr<OperatorTask>> update_district(const int w_id, const int d_id,
                                                             const double payment_amount) {
    /**
     * UPDATE district SET d_ytd = d_ytd + :h_amount
     * WHERE d_w_id=:w_id AND d_id=:d_id;
     */

    return std::vector<std::shared_ptr<OperatorTask>>();
  }

  std::vector<std::shared_ptr<OperatorTask>> get_district(const int w_id, const int d_id) {
    /**
     * SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
     * FROM district
     * WHERE d_w_id=:w_id AND d_id=:d_id;
     */

    return std::vector<std::shared_ptr<OperatorTask>>();
  }

  std::vector<std::shared_ptr<OperatorTask>> get_customer_by_name(const std::string c_last, const int c_d_id,
                                                                  const int c_w_id) {
    /**
     * SELECT c_first, c_middle, c_id,
     * c_street_1, c_street_2, c_city, c_state, c_zip,
     * c_phone, c_credit, c_credit_lim,
     * c_discount, c_balance, c_since
     * FROM customer
     * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
     * ORDER BY c_first;
     */
    auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
    auto first_filter = std::make_shared<TableScan>(gt_customer, "C_LAST", "=", c_last);
    auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
    auto projection = std::make_shared<Projection>(
        third_filter, std::vector<std::string>{"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_ID"});
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

  std::vector<std::shared_ptr<OperatorTask>> get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) {
    /**
      * SELECT c_first, c_middle, c_last,
      * c_street_1, c_street_2, c_city, c_state, c_zip,
      * c_phone, c_credit, c_credit_lim,
      * c_discount, c_balance, c_since
      * FROM customer
      * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
      */
    auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
    auto first_filter = std::make_shared<TableScan>(gt_customer, "C_ID", "=", c_id);
    auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
    auto projection = std::make_shared<Projection>(
        third_filter, std::vector<std::string>{"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_LAST"});

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

  std::vector<std::shared_ptr<OperatorTask>> get_customer_data(const int c_id, const int c_d_id, const int c_w_id) {
    /**
     * SELECT c_data
     * FROM customer
     * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
     */
    auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
    auto first_filter = std::make_shared<TableScan>(gt_customer, "C_ID", "=", c_id);
    auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
    auto projection = std::make_shared<Projection>(third_filter, std::vector<std::string>{"C_DATA"});

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

  std::vector<std::shared_ptr<OperatorTask>> update_customer(const int c_id, const int c_d_id, const int c_w_id) {
    /**
     * UPDATE customer SET c_balance = :c_balance
     * WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND
     * c_id = :c_id;
     */

    /**
     * UPDATE customer
     * SET c_balance = :c_balance, c_data = :c_new_data
     * WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND
     * c_id = :c_id;
     */

    return std::vector<std::shared_ptr<OperatorTask>>();
  }

  std::vector<std::shared_ptr<OperatorTask>> insert_history(const int c_id, const int c_d_id, const int c_w_id) {
    /**
     * INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,
     * h_w_id, h_date, h_amount, h_data)
     * VALUES (:c_d_id, :c_w_id, :c_id, :d_id,
     * :w_id, :datetime, :h_amount, :h_data);
     */

    return std::vector<std::shared_ptr<OperatorTask>>();
  }
};

BENCHMARK_F(TPCCPaymentBenchmark, BM_TPCC_Payment)(benchmark::State &state) {
  clear_cache();

  auto home_warehouse_id = 0;  // there is only one warehouse

  while (state.KeepRunning()) {
    // pass in i>1000 to trigger random value generation
    auto c_last = _random_gen.last_name(2000);
    auto d_id = _random_gen.number(1, 10);
    auto c_id = _random_gen.nurand(1023, 1, 3000);
    auto use_last_name = _random_gen.number(0, 100) < 60;
    // irrelevant since there is only one warehouse
    //    auto use_home_warehouse = _random_gen.number(0, 100) < 85;
    auto payment_amount = _random_gen.number(100, 500000) / 100.0;
    //    auto payment_date = std::time(0);

    auto t_context = TransactionManager::get().new_transaction_context();

    auto update_warehouse_tasks = update_warehouse(home_warehouse_id, payment_amount);
    AbstractScheduler::schedule_tasks_and_wait(update_warehouse_tasks);

    auto get_warehouse_tasks = get_warehouse(home_warehouse_id);
    AbstractScheduler::schedule_tasks_and_wait(get_warehouse_tasks);

    auto update_district_tasks = update_district(home_warehouse_id, d_id, payment_amount);
    AbstractScheduler::schedule_tasks_and_wait(update_district_tasks);

    auto get_district_tasks = get_district(home_warehouse_id, d_id);
    AbstractScheduler::schedule_tasks_and_wait(get_district_tasks);

    if (use_last_name) {
      auto get_customer_tasks = get_customer_by_name(c_last, d_id, home_warehouse_id);
      AbstractScheduler::schedule_tasks_and_wait(get_customer_tasks);

      //      auto num_names = get_customer_tasks.back()->get_operator()->get_output()->row_count();
      //      assert(num_names > 0);

      //      auto customer =
      //          get_from_table_at_row(get_customer_tasks.back()->get_operator()->get_output(), ceil(num_names / 2));

      // locate midpoint customer
    } else {
      auto get_customer_tasks = get_customer_by_id(c_id, d_id, home_warehouse_id);
      AbstractScheduler::schedule_tasks_and_wait(get_customer_tasks);

      //      assert(get_customer_tasks.back()->get_operator()->get_output()->row_count() == 1);
      //      auto customer = get_from_table_at_row(get_customer_tasks.back()->get_operator()->get_output(), 0);

      /**
       * c_balance += h_amount;
       * c_credit[2]='\ 0';
       * if (strstr(c_credit, "BC") )
       */
      {
        auto get_customer_data_tasks = get_customer_data(c_id, d_id, home_warehouse_id);
        AbstractScheduler::schedule_tasks_and_wait(get_customer_data_tasks);

        auto update_customer_tasks = update_customer(c_id, home_warehouse_id, d_id);
        AbstractScheduler::schedule_tasks_and_wait(update_customer_tasks);
      }  // else
      {
        auto update_customer_tasks = update_customer(c_id, home_warehouse_id, d_id);
        AbstractScheduler::schedule_tasks_and_wait(update_customer_tasks);
      }

      auto insert_history_tasks = insert_history(d_id, home_warehouse_id, c_id /*, d_id, payment_amount, some data*/);
      AbstractScheduler::schedule_tasks_and_wait(insert_history_tasks);

      // Commit transaction.
      TransactionManager::get().prepare_commit(*t_context);

      auto commit = std::make_shared<CommitRecords>();
      commit->set_transaction_context(t_context);

      auto commit_task = std::make_shared<OperatorTask>(commit);
      commit_task->schedule();
      commit_task->join();

      TransactionManager::get().commit(*t_context);
    }
  }
}

}  // namespace opossum
