#include <ctime>
#include <random>

#include "tpcc_payment.hpp"

namespace opossum {

TpccPayment::TpccPayment(const int num_warehouses) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  // There are always exactly 9 districts per warehouse
  // TODO this should be [1, 10], but our data generator does [0, 9]
  std::uniform_int_distribution<> district_dist{0, 9};
	_d_id = district_dist(_random_engine);

  // Use home warehouse in 85% of cases, otherwise select a random one
  // TODO Deal with single warehouse
  std::uniform_int_distribution<> home_warehouse_dist{1, 100};
  _c_w_id = _w_id;
  if (home_warehouse_dist(_random_engine) > 85) {
    do {
      _c_w_id = warehouse_dist(_random_engine);
    } while (_c_w_id == _w_id);
  }

  // Select 6 out of 10 customers by last name
  std::uniform_int_distribution<> customer_selection_method_dist{1, 10};
  _select_customer_by_name = customer_selection_method_dist(_random_engine) <= 6;
  if (_select_customer_by_name) {
    _customer = pmr_string{_tpcc_random_generator.last_name(_tpcc_random_generator.nurand(255, 0, 999))};
  } else {
    _customer = _tpcc_random_generator.nurand(1023, 1, 3000);
  }

  // Generate payment information
  std::uniform_real_distribution<float> amount_dist{1.f, 5000.f};
  _h_amount = amount_dist(_random_engine);
  _h_date = std::time(nullptr);
}

void TpccPayment::execute() {
  // Retrieve information about the warehouse
  const auto warehouse_table = _execute_sql(std::string{"SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD FROM WAREHOUSE WHERE W_ID = "} + std::to_string(_w_id));
  DebugAssert(warehouse_table->row_count() == 1, "Did not find warehouse (or found more than one)");
  _w_name = warehouse_table->get_value<float>(ColumnID{0}, 0);
  _w_ytd = warehouse_table->get_value<float>(ColumnID{6}, 0);

  // Update warehouse YTD
  _execute_sql(std::string{"UPDATE WAREHOUSE SET W_YTD = "} + std::to_string(_w_ytd + _h_amount) + " WHERE W_ID = " + std::to_string(_w_id));

  // Retrieve information about the district
  const auto district_table = _execute_sql(std::string{"SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD FROM WAREHOUSE WHERE D_W_ID = "} + std::to_string(_w_id) + " AND D_ID = " std::to_string(_d_id));
  DebugAssert(district_table->row_count() == 1, "Did not find district (or found more than one)");
  _w_name = district_table->get_value<float>(ColumnID{0}, 0);
  _w_ytd = district_table->get_value<float>(ColumnID{6}, 0);

  // Update district YTD
  _execute_sql(std::string{"UPDATE DISTRICT SET D_YTD = "} + std::to_string(_w_ytd + _h_amount) + " WHERE D_W_ID = " + std::to_string(_w_id) + " AND D_ID = " std::to_string(_d_id));


  _transaction_context->commit();
}

std::ostream& TpccPayment::print(std::ostream& stream) const {
  stream << "Payment";
  return stream;
}

}
