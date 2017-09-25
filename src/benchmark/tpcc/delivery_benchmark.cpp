#include <cassert>
#include <ctime>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "tpcc_base_fixture.hpp"

#include "concurrency/transaction_manager.hpp"
#include "operators/aggregate.hpp"
#include "operators/commit_records.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "scheduler/operator_task.hpp"

#include "tpcc/constants.hpp"
#include "tpcc/helper.hpp"

#include "types.hpp"

namespace opossum {

class TPCCDeliveryBenchmark : public TPCCBenchmarkFixture {
 public:
  inline std::vector<std::shared_ptr<OperatorTask>> get_new_order_id(const int d_id, const int w_id) {
    /**
     * EXEC SQL DECLARE c_no CURSOR FOR
     * SELECT no_o_id
     * FROM new_order
     * WHERE no_d_id = :d_id AND no_w_id = :w_id ORDER BY no_o_id ASC;
     */
    auto gt = std::make_shared<GetTable>("NEW-ORDER");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{1} /* "NO_D_ID" */, ScanType::OpEquals, d_id);
    auto ts2 = std::make_shared<TableScan>(ts1, ColumnID{2} /* "NO_W_ID" */, ScanType::OpEquals, w_id);
    auto ts3 = std::make_shared<TableScan>(ts2, ColumnID{0} /* "NO_O_ID" */, ScanType::OpGreaterThan, -1);
    auto val = std::make_shared<Validate>(ts3);

    Projection::ColumnExpressions columns = {Expression::create_column(ColumnID{0} /* "NO_O_ID" */)};
    auto projection = std::make_shared<Projection>(val, columns);

    auto sort = std::make_shared<Sort>(projection, ColumnID{0} /* "NO_O_ID" */, OrderByMode::Ascending, 0);

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_ts2 = std::make_shared<OperatorTask>(ts2);
    auto t_ts3 = std::make_shared<OperatorTask>(ts3);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_projection = std::make_shared<OperatorTask>(projection);
    auto t_sort = std::make_shared<OperatorTask>(sort);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_ts2);
    t_ts2->set_as_predecessor_of(t_ts3);
    t_ts3->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_projection);
    t_projection->set_as_predecessor_of(t_sort);

    return {t_gt, t_ts1, t_ts2, t_ts3, t_val, t_projection, t_sort};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> delete_from_new_order(const int no_o_id) {
    /**
     * EXEC SQL DELETE
     * FROM new_order
     * WHERE CURRENT OF c_no;
     */
    auto gt = std::make_shared<GetTable>("NEW-ORDER");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{0} /* "NO_O_ID" */, ScanType::OpEquals, no_o_id);
    auto val = std::make_shared<Validate>(ts1);
    auto delete_op = std::make_shared<Delete>("NEW-ORDER", val);

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_delete_op = std::make_shared<OperatorTask>(delete_op);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_delete_op);

    return {t_gt, t_ts1, t_val, t_delete_op};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> get_order_id(const int d_id, const int w_id, const int no_o_id) {
    /**
     * EXEC SQL SELECT o_c_id INTO :c_id
     * FROM orders
     * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
     */
    auto gt = std::make_shared<GetTable>("ORDER");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{0} /* "O_ID" */, ScanType::OpEquals, no_o_id);
    auto ts2 = std::make_shared<TableScan>(ts1, ColumnID{1} /* "O_D_ID" */, ScanType::OpEquals, d_id);
    auto ts3 = std::make_shared<TableScan>(ts2, ColumnID{2} /* "O_W_ID" */, ScanType::OpEquals, w_id);
    auto val = std::make_shared<Validate>(ts3);

    Projection::ColumnExpressions columns = {Expression::create_column(ColumnID{3} /* "O_C_ID" */)};
    auto projection = std::make_shared<Projection>(val, columns);

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_ts2 = std::make_shared<OperatorTask>(ts2);
    auto t_ts3 = std::make_shared<OperatorTask>(ts3);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_projection = std::make_shared<OperatorTask>(projection);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_ts2);
    t_ts2->set_as_predecessor_of(t_ts3);
    t_ts3->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_projection);

    return {t_gt, t_ts1, t_ts2, t_ts3, t_val, t_projection};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> update_order(const int d_id, const int w_id, const int no_o_id,
                                                                 const int o_carrier_id) {
    /**
     * EXEC SQL UPDATE orders
     * SET o_carrier_id = :o_carrier_id
     * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
     */
    auto gt = std::make_shared<GetTable>("ORDER");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{0} /* "O_ID" */, ScanType::OpEquals, no_o_id);
    auto ts2 = std::make_shared<TableScan>(ts1, ColumnID{1} /* "O_D_ID" */, ScanType::OpEquals, d_id);
    auto ts3 = std::make_shared<TableScan>(ts2, ColumnID{2} /* "O_W_ID" */, ScanType::OpEquals, w_id);

    auto val = std::make_shared<Validate>(ts3);

    Projection::ColumnExpressions columns = {Expression::create_column(ColumnID{5} /* "O_CARRIER_ID" */)};

    auto projection = std::make_shared<Projection>(val, columns);

    Projection::ColumnExpressions values = {Expression::create_literal(o_carrier_id, {"O_CARRIER_ID"})};
    auto updated_rows = std::make_shared<Projection>(val, values);
    auto update = std::make_shared<Update>("ORDER", projection, updated_rows);

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_ts2 = std::make_shared<OperatorTask>(ts2);
    auto t_ts3 = std::make_shared<OperatorTask>(ts3);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_projection = std::make_shared<OperatorTask>(projection);
    auto t_updated_rows = std::make_shared<OperatorTask>(updated_rows);
    auto t_update = std::make_shared<OperatorTask>(update);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_ts2);
    t_ts2->set_as_predecessor_of(t_ts3);
    t_ts3->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_projection);
    t_val->set_as_predecessor_of(t_updated_rows);
    t_projection->set_as_predecessor_of(t_update);
    t_updated_rows->set_as_predecessor_of(t_update);

    return {t_gt, t_ts1, t_ts2, t_ts3, t_val, t_projection, t_updated_rows, t_update};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> update_order_line(const int d_id, const int w_id, const int no_o_id,
                                                                      const time_t datetime) {
    /**
     * EXEC SQL UPDATE order_line
     * SET ol_delivery_d = :datetime
     * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
     */
    auto gt = std::make_shared<GetTable>("ORDER-LINE");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{0} /* "OL_O_ID" */, ScanType::OpEquals, no_o_id);
    auto ts2 = std::make_shared<TableScan>(ts1, ColumnID{1} /* "OL_D_ID" */, ScanType::OpEquals, d_id);
    auto ts3 = std::make_shared<TableScan>(ts2, ColumnID{2} /* "OL_W_ID" */, ScanType::OpEquals, w_id);

    auto val = std::make_shared<Validate>(ts3);

    Projection::ColumnExpressions columns = {Expression::create_column(ColumnID{6} /* "OL_DELIVERY_D" */)};
    auto projection = std::make_shared<Projection>(val, columns);

    Projection::ColumnExpressions values = {Expression::create_literal(std::to_string(datetime), {"OL_DELIVERY_D"})};
    auto updated_rows = std::make_shared<Projection>(val, values);
    auto update = std::make_shared<Update>("ORDER-LINE", projection, updated_rows);

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_ts2 = std::make_shared<OperatorTask>(ts2);
    auto t_ts3 = std::make_shared<OperatorTask>(ts3);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_projection = std::make_shared<OperatorTask>(projection);
    auto t_updated_rows = std::make_shared<OperatorTask>(updated_rows);
    auto t_update = std::make_shared<OperatorTask>(update);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_ts2);
    t_ts2->set_as_predecessor_of(t_ts3);
    t_ts3->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_projection);
    t_val->set_as_predecessor_of(t_updated_rows);
    t_projection->set_as_predecessor_of(t_update);
    t_updated_rows->set_as_predecessor_of(t_update);

    return {t_gt, t_ts1, t_ts2, t_ts3, t_val, t_projection, t_updated_rows, t_update};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> sum_of_order_line(const int d_id, const int w_id,
                                                                      const int no_o_id) {
    /**
     * EXEC SQL SELECT SUM(ol_amount) INTO :ol_total
     * FROM order_line
     * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
     */
    auto gt = std::make_shared<GetTable>("ORDER-LINE");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{0} /* "OL_O_ID" */, ScanType::OpEquals, no_o_id);
    auto ts2 = std::make_shared<TableScan>(ts1, ColumnID{1} /* "OL_D_ID" */, ScanType::OpEquals, d_id);
    auto ts3 = std::make_shared<TableScan>(ts2, ColumnID{2} /* "OL_W_ID" */, ScanType::OpEquals, w_id);

    auto val = std::make_shared<Validate>(ts3);

    auto sum = std::make_shared<Aggregate>(
        val, std::vector<AggregateDefinition>{{ColumnID{8} /* "OL_AMOUNT" */, AggregateFunction::Sum}},
        std::vector<ColumnID>{});

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_ts2 = std::make_shared<OperatorTask>(ts2);
    auto t_ts3 = std::make_shared<OperatorTask>(ts3);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_sum = std::make_shared<OperatorTask>(sum);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_ts2);
    t_ts2->set_as_predecessor_of(t_ts3);
    t_ts3->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_sum);

    return {t_gt, t_ts1, t_ts2, t_ts3, t_val, t_sum};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> update_customer(const float ol_total, const int d_id,
                                                                    const int w_id, const int c_id) {
    /**
     * EXEC SQL UPDATE customer
     * SET c_balance = c_balance + :ol_total
     * WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
     */
    auto gt = std::make_shared<GetTable>("CUSTOMER");

    auto ts1 = std::make_shared<TableScan>(gt, ColumnID{0} /* "C_ID" */, ScanType::OpEquals, c_id);
    auto ts2 = std::make_shared<TableScan>(ts1, ColumnID{1} /* "C_D_ID" */, ScanType::OpEquals, d_id);
    auto ts3 = std::make_shared<TableScan>(ts2, ColumnID{2} /* "C_W_ID" */, ScanType::OpEquals, w_id);

    auto val = std::make_shared<Validate>(ts3);

    Projection::ColumnExpressions columns = {Expression::create_column(ColumnID{16} /* "C_BALANCE" */)};
    auto projection = std::make_shared<Projection>(val, columns);

    Projection::ColumnExpressions values = {Expression::create_binary_operator(
        ExpressionType::Addition, Expression::create_column(ColumnID{16}), Expression::create_literal(ol_total))};
    auto updated_rows = std::make_shared<Projection>(val, values);
    auto update = std::make_shared<Update>("CUSTOMER", projection, updated_rows);

    auto t_gt = std::make_shared<OperatorTask>(gt);
    auto t_ts1 = std::make_shared<OperatorTask>(ts1);
    auto t_ts2 = std::make_shared<OperatorTask>(ts2);
    auto t_ts3 = std::make_shared<OperatorTask>(ts3);
    auto t_val = std::make_shared<OperatorTask>(val);
    auto t_projection = std::make_shared<OperatorTask>(projection);
    auto t_updated_rows = std::make_shared<OperatorTask>(updated_rows);
    auto t_update = std::make_shared<OperatorTask>(update);

    t_gt->set_as_predecessor_of(t_ts1);
    t_ts1->set_as_predecessor_of(t_ts2);
    t_ts2->set_as_predecessor_of(t_ts3);
    t_ts3->set_as_predecessor_of(t_val);
    t_val->set_as_predecessor_of(t_projection);
    t_val->set_as_predecessor_of(t_updated_rows);
    t_projection->set_as_predecessor_of(t_update);
    t_updated_rows->set_as_predecessor_of(t_update);

    return {t_gt, t_ts1, t_ts2, t_ts3, t_val, t_projection, t_updated_rows, t_update};
  }

  inline std::vector<std::shared_ptr<OperatorTask>> commit() {
    auto commit = std::make_shared<CommitRecords>();
    return {std::make_shared<OperatorTask>(commit)};
  }
};

BENCHMARK_F(TPCCDeliveryBenchmark, BM_delivery)(benchmark::State& state) {
  clear_cache();

  // currently no warm up

  int w_id = static_cast<int32_t>(_random_gen.random_number(0, _gen._warehouse_size - 1));
  int d_id = 0;
  while (state.KeepRunning()) {
    auto t_context = TransactionManager::get().new_transaction_context();
    d_id = (d_id + 1) % tpcc::NUM_DISTRICTS_PER_WAREHOUSE;
    int o_carrier_id = _random_gen.random_number(1, 10);
    const time_t datetime = std::time(0);
    auto tasks = get_new_order_id(d_id, w_id);
    tpcc::execute_tasks_with_context(tasks, t_context);

    assert(tasks.back()->get_operator()->get_output()->row_count() > 0);
    auto no_o_id = tasks.back()->get_operator()->get_output()->get_value<int>(opossum::ColumnID(0u), 0u);
    tasks = delete_from_new_order(no_o_id);
    tpcc::execute_tasks_with_context(tasks, t_context);

    tasks = get_order_id(d_id, w_id, no_o_id);
    tpcc::execute_tasks_with_context(tasks, t_context);

    assert(tasks.back()->get_operator()->get_output()->row_count() > 0);
    auto c_id = tasks.back()->get_operator()->get_output()->get_value<int>(opossum::ColumnID(0u), 0u);

    tasks = update_order(d_id, w_id, no_o_id, o_carrier_id);
    tpcc::execute_tasks_with_context(tasks, t_context);

    tasks = update_order_line(d_id, w_id, no_o_id, datetime);
    tpcc::execute_tasks_with_context(tasks, t_context);

    tasks = sum_of_order_line(d_id, w_id, no_o_id);
    tpcc::execute_tasks_with_context(tasks, t_context);

    assert(tasks.back()->get_operator()->get_output()->row_count() > 0);
    auto ol_total =
        static_cast<float>(tasks.back()->get_operator()->get_output()->get_value<double>(opossum::ColumnID(0u), 0u));
    tasks = update_customer(ol_total, d_id, w_id, c_id);
    tpcc::execute_tasks_with_context(tasks, t_context);

    // Commit transaction.
    TransactionManager::get().prepare_commit(*t_context);
    tasks = commit();
    tpcc::execute_tasks_with_context(tasks, t_context);
    TransactionManager::get().commit(*t_context);
  }
}

}  // namespace opossum
