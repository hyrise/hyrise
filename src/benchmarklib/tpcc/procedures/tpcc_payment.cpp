#include <ctime>
#include <random>

#include "tpcc_payment.hpp"

namespace opossum {

TPCCPayment::TPCCPayment(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor) {
  std::uniform_int_distribution<> warehouse_dist{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  std::uniform_int_distribution<> district_dist{1, 10};
  d_id = district_dist(_random_engine);

  // Use home warehouse in 85% of cases, otherwise select a random one
  std::uniform_int_distribution<> home_warehouse_dist{1, 100};
  c_w_id = w_id;
  c_d_id = d_id;
  if (num_warehouses > 2 && home_warehouse_dist(_random_engine) > 85) {
    // Choose remote warehouse
    do {
      c_w_id = warehouse_dist(_random_engine);
    } while (c_w_id == w_id);
    c_d_id = district_dist(_random_engine);
  }

  // Select 6 out of 10 customers by last name
  std::uniform_int_distribution<> customer_selection_method_dist{1, 10};
  select_customer_by_name = customer_selection_method_dist(_random_engine) <= 6;
  if (select_customer_by_name) {
    customer = pmr_string{_tpcc_random_generator.last_name(_tpcc_random_generator.nurand(255, 0, 999))};
  } else {
    customer = static_cast<int32_t>(_tpcc_random_generator.nurand(1023, 1, 3000));
  }

  // Generate payment information
  std::uniform_real_distribution<float> amount_dist{1.f, 5000.f};
  h_amount = amount_dist(_random_engine);
  h_date = static_cast<int32_t>(std::time(nullptr));
}

bool TPCCPayment::_on_execute() {
  SQLPipelineStatus pipeline_status;

  // Retrieve information about the warehouse
  const auto warehouse_select_pair = _sql_executor.execute(
      std::string{"SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD FROM WAREHOUSE WHERE W_ID = "} +
      std::to_string(w_id));
  const auto& warehouse_table = warehouse_select_pair.second;
  Assert(warehouse_table && warehouse_table->row_count() == 1, "Did not find warehouse (or found more than one)");
  auto w_name = warehouse_table->get_value<pmr_string>(ColumnID{0}, 0);
  auto w_ytd = warehouse_table->get_value<float>(ColumnID{6}, 0);

  // Update warehouse YTD
  std::tie(pipeline_status, std::ignore) =
      _sql_executor.execute(std::string{"UPDATE WAREHOUSE SET W_YTD = "} + std::to_string(w_ytd + h_amount) +
                            " WHERE W_ID = " + std::to_string(w_id));
  if (pipeline_status != SQLPipelineStatus::Success) {
    return false;
  }

  // Retrieve information about the district
  const auto district_select_pair = _sql_executor.execute(
      std::string{
          "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD FROM DISTRICT WHERE D_W_ID = "} +
      std::to_string(w_id) + " AND D_ID = " + std::to_string(d_id));
  const auto& district_table = district_select_pair.second;
  Assert(district_table && district_table->row_count() == 1, "Did not find district (or found more than one)");
  auto d_name = district_table->get_value<pmr_string>(ColumnID{0}, 0);
  auto d_ytd = district_table->get_value<float>(ColumnID{6}, 0);

  // Update district YTD
  const auto district_update_pair =
      _sql_executor.execute(std::string{"UPDATE DISTRICT SET D_YTD = "} + std::to_string(d_ytd + h_amount) +
                            " WHERE D_W_ID = " + std::to_string(w_id) + " AND D_ID = " + std::to_string(d_id));
  if (district_update_pair.first != SQLPipelineStatus::Success) {
    return false;
  }

  auto customer_table = std::shared_ptr<const Table>{};
  auto customer_offset = size_t{};

  if (!select_customer_by_name) {
    // Case 1 - Select customer by ID
    std::tie(std::ignore, customer_table) = _sql_executor.execute(
        std::string{"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, "
                    "C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA FROM CUSTOMER WHERE C_W_ID = "} +
        std::to_string(w_id) + " AND C_D_ID = " + std::to_string(c_d_id) +
        " AND C_ID = " + std::to_string(std::get<int32_t>(customer)));
    Assert(customer_table && customer_table->row_count() == 1, "Did not find customer by ID (or found more than one)");

    customer_offset = size_t{0};
    c_id = std::get<int32_t>(customer);
  } else {
    // Case 2 - Select customer by name
    std::tie(std::ignore, customer_table) = _sql_executor.execute(
        std::string{"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, "
                    "C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA FROM CUSTOMER WHERE C_W_ID = "} +
        std::to_string(w_id) + " AND C_D_ID = " + std::to_string(c_d_id) + " AND C_LAST = '" +
        std::string{std::get<pmr_string>(customer)} + "' ORDER BY C_FIRST");
    Assert(customer_table && customer_table->row_count() >= 1, "Did not find customer by name");

    // Calculate ceil(n/2)
    customer_offset =
        static_cast<size_t>(std::max(0.0, std::min(std::ceil(customer_table->row_count() / 2.0),
                                                   static_cast<double>(customer_table->row_count() - 1))));
    c_id = customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);
  }

  // There is a possible optimization here if we take `customer_table` as an input to an UPDATE operator, but that
  // would be outside of the SQL realm. Also, it would it make it impossible to run _execute_sql via the network
  // layer later on.
  const auto customer_update_balance_pair =
      _sql_executor.execute(std::string{"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE - "} + std::to_string(h_amount) +
                            ", C_YTD_PAYMENT = C_YTD_PAYMENT + " + std::to_string(h_amount) +
                            ", C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = " + std::to_string(w_id) +
                            " AND C_D_ID = " + std::to_string(c_d_id) + " AND C_ID = " + std::to_string(c_id));
  if (customer_update_balance_pair.first != SQLPipelineStatus::Success) {
    return false;
  }

  // Retrieve C_CREDIT and check for "bad credit"
  if (customer_table->get_value<pmr_string>(ColumnID{11}, customer_offset) == "BC") {
    std::stringstream new_c_data_stream;
    new_c_data_stream << customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);  // C_ID
    new_c_data_stream << c_d_id;
    new_c_data_stream << c_w_id;
    new_c_data_stream << d_id;
    new_c_data_stream << w_id;
    new_c_data_stream << h_amount;
    new_c_data_stream << customer_table->get_value<pmr_string>(ColumnID{15}, customer_offset);  // C_DATA
    auto new_c_data = new_c_data_stream.str();
    new_c_data.resize(std::min(new_c_data.size(), size_t{500}));
    const auto customer_update_data_pair = _sql_executor.execute(
        std::string{"UPDATE CUSTOMER SET C_DATA = '"} + new_c_data + "' WHERE C_W_ID = " + std::to_string(w_id) +
        " AND C_D_ID = " + std::to_string(c_d_id) + " AND C_ID = " + std::to_string(c_id));
    if (customer_update_data_pair.first != SQLPipelineStatus::Success) {
      return false;
    }
  }

  // Insert into history table
  const auto history_insert_pair = _sql_executor.execute(std::string{
      "INSERT INTO HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATA, H_DATE, H_AMOUNT) VALUES (" +
      std::to_string(c_id) + ", " + std::to_string(c_d_id) + ", " + std::to_string(c_w_id) + ", " +
      std::to_string(d_id) + ", " + std::to_string(w_id) + ", '" + std::string{w_name + "    " + d_name} + "', '" +
      std::to_string(h_date) + "', " + std::to_string(h_amount) + ")"});
  Assert(history_insert_pair.first == SQLPipelineStatus::Success, "INSERT should not fail");

  _sql_executor.commit();
  return true;
}

}  // namespace opossum
