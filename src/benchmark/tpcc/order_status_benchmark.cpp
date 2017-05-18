#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/aggregate.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/scheduler/operator_task.hpp"
#include "../../lib/storage/storage_manager.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

inline int32_t get_name_count(std::string c_last, int c_d_id, int c_w_id) {
  /**
   * EXEC SQL SELECT count(c_id) INTO :namecnt
   * FROM customer
   * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
   */

  StorageManager::get().print();
  auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
  auto first_filter = std::make_shared<TableScan>(gt_customer, "C_LAST", "=", c_last);
  auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
  auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
  // TODO(anyone): replace Sum by Count as soon as merged
  auto count = std::make_shared<Aggregate>(
      third_filter, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("C_ID"), Sum)},
      std::vector<std::string>{"C_D_ID"});

  auto gt_customer_task = std::make_shared<OperatorTask>(gt_customer);
  auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
  auto count_task = std::make_shared<OperatorTask>(count);

  gt_customer_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(count_task);

  gt_customer_task->schedule();
  first_filter_task->schedule();
  second_filter_task->schedule();
  third_filter_task->schedule();
  count_task->schedule();

  count_task->join();

  auto output_operator = count_task->get_operator();

  std::cout << "Rowcount for Customer " << gt_customer_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for First " << first_filter_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Second " << second_filter_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Third " << third_filter_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Count " << count_task->get_operator()->get_output()->row_count() << std::endl;

  std::cout << "----------" << std::endl;

  // get count from COUNT output
  auto output_table = output_operator->get_output();
  std::cout << "rowcount " << output_table->row_count() << std::endl;

  if (output_table->row_count() > 0) {
    // TODO(anyone): switch to COUNT(C_ID) as soon as available
    auto count_column_id = output_table->column_id_by_name(std::string("SUM(C_ID)"));
    auto count_result = output_table->get_chunk(0).get_column(count_column_id)->operator[](0);

    std::cout << "Count result " << count_result << std::endl;
  }

  return 0;
}

inline std::shared_ptr<const Table> get_customer(std::string c_last, int c_d_id, int c_w_id) {
  /**
   * EXEC SQL DECLARE c_name CURSOR FOR
   * SELECT c_balance, c_first, c_middle, c_id
   * FROM customer
   * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
   * ORDER BY c_first;
   * EXEC SQL OPEN c_name;
   */
  StorageManager::get().print();
  auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
  auto first_filter = std::make_shared<TableScan>(gt_customer, "C_LAST", "=", c_last);
  auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
  auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
  auto sort = std::make_shared<Sort>(third_filter, "C_FIRST", true);
  std::vector<std::string> columns = {"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_ID"};
  auto projection = std::make_shared<Projection>(sort, columns);

  auto gt_customer_task = std::make_shared<OperatorTask>(gt_customer);
  auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
  auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
  auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
  auto sort_task = std::make_shared<OperatorTask>(sort);
  auto projection_task = std::make_shared<OperatorTask>(projection);

  gt_customer_task->set_as_predecessor_of(first_filter_task);
  first_filter_task->set_as_predecessor_of(second_filter_task);
  second_filter_task->set_as_predecessor_of(third_filter_task);
  third_filter_task->set_as_predecessor_of(sort_task);
  sort_task->set_as_predecessor_of(projection_task);

  gt_customer_task->schedule();
  first_filter_task->schedule();
  second_filter_task->schedule();
  third_filter_task->schedule();
  sort_task->schedule();
  projection_task->schedule();

  projection_task->join();

  std::cout << "Rowcount for Customer " << gt_customer_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for First " << first_filter_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Second " << second_filter_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Third " << third_filter_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Sort " << sort_task->get_operator()->get_output()->row_count() << std::endl;
  std::cout << "Rowcount for Projection " << projection_task->get_operator()->get_output()->row_count() << std::endl;

  std::cout << "----------" << std::endl;

  // get count from COUNT output
  auto output_table = projection_task->get_operator()->get_output();
  //  std::cout << "rowcount " << output_table->row_count() << std::endl;
  //  // TODO: switch to COUNT(C_ID) as soon as available
  //  auto count_column_id = output_table->column_id_by_name(std::string("SUM(C_ID)"));
  //  auto count_result = output_table->get_chunk(0).get_column(count_column_id)->operator[](0);
  //
  //  std::cout << "Count result " << count_result << std::endl;

  return output_table;
}

BENCHMARK_F(TPCCBenchmarkFixture, BM_OrderStatus)(benchmark::State& state) {
  clear_cache();
  //  auto warm_up = std::make_shared<Difference>(_gt_a, _gt_b);
  //  warm_up->execute();
  while (state.KeepRunning()) {
    auto y = _random_gen.number(0, 99);
    bool byLastname = y < 60;

    // pass in i>1000 to trigger random value generation
    auto c_last = _random_gen.last_name(2000);
    size_t c_d_id = _random_gen.number(1, 10);
    size_t c_w_id = 0;  // there is only one warehouse
                        //    size_t c_id = randomGenerator.nurand(1023, 1, 3000);

    if (byLastname) {
      auto namecount = get_name_count(c_last, c_d_id, c_w_id);
      std::cout << "namecout " << namecount << std::endl;

      auto customer_result = get_customer(c_last, c_d_id, c_w_id);

      std::cout << "customer result " << customer_result->row_count() << std::endl;

      if (namecount % 2) {
        namecount++;
      }

      for (int n = 0; n < namecount / 2; n++) {
      }

      /**
       * if (namecnt%2) namecnt++; / / Locate midpoint customer
       * for (n=0; n<namecnt/ 2; n++)
       * {
       * EXEC SQL FETCH c_name
       * INTO :c_balance, :c_first, :c_middle, :c_id;
       * }
       * EXEC SQL CLOSE c_name;
       */
    } else {
      /**
       * EXEC SQL SELECT c_balance, c_first, c_middle, c_last
       * INTO :c_balance, :c_first, :c_middle, :c_last
       * FROM customer
       * WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
       */
    }

    /**
     * EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
     * INTO :o_id, :o_carrier_id, :entdate
     * FROM orders
     * ORDER BY o_id DESC;
     */

    /**
     * EXEC SQL DECLARE c_line CURSOR FOR
     * SELECT ol_i_id, ol_supply_w_id, ol_quantity,
     * ol_amount, ol_delivery_d
     * FROM order_line
     * WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
     */

    /**
     * i=0;
     * while (sql_notfound(FALSE))
     * {
     * i++;
     * EXEC SQL FETCH c_line
     * INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i],
     * :ol_amount[i], :ol_delivery_d[i];
     * }
     */
  }
}

}  // namespace opossum
