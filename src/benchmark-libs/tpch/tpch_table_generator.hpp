#pragma once

#include <time.h>
#include <memory>
#include <string>
#include <vector>

#include "random_generator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "text_field_generator.hpp"

namespace tpch {

class TableGenerator {
  // following TPC-H v2.17.2
 public:
  struct OrderLine {
    size_t orderkey;  // same for all orderlines of same order
    size_t partkey;
    size_t quantity;
    float extendedprice;
    float discount;
    float tax;
    size_t orderdate;  // same for all orderlines of same order
    size_t shipdate;
    size_t receiptdate;
    std::string linestatus;
  };

  TableGenerator();

  virtual ~TableGenerator() = default;

  std::shared_ptr<opossum::Table> generate_suppliers_table();

  std::shared_ptr<opossum::Table> generate_parts_table();

  std::shared_ptr<opossum::Table> generate_partsupps_table();

  std::shared_ptr<opossum::Table> generate_customers_table();

  typedef std::shared_ptr<std::vector<std::vector<OrderLine>>> order_lines_type;

  order_lines_type generate_order_lines();

  std::shared_ptr<opossum::Table> generate_orders_table(order_lines_type order_lines);

  std::shared_ptr<opossum::Table> generate_lineitems_table(order_lines_type order_lines);

  std::shared_ptr<opossum::Table> generate_nations_table();

  std::shared_ptr<opossum::Table> generate_regions_table();

  void add_all_tables(opossum::StorageManager &manager);

  const size_t _chunk_size = 1000;

  const size_t _scale_factor = 1;

  const size_t _supplier_size = 10000;   // * _scale_factor
  const size_t _part_size = 200000;      // * _scale_factor
  const size_t _partsupp_size = 4;       // per part
  const size_t _customer_size = 150000;  // * _scale_factor
  const size_t _order_size = 10;         // per customer
  const size_t _nation_size = 25;
  const size_t _region_size = 5;

 protected:
  template <typename T>
  std::shared_ptr<opossum::ValueColumn<T>> add_column(size_t cardinality,
                                                      const std::function<T(size_t)> &generator_function);

  float calculate_part_retailprice(size_t i) const;

  size_t get_time(size_t year, size_t month, size_t day) {
    struct std::tm time;
    time.tm_sec = 0;
    time.tm_min = 0;
    time.tm_hour = 0;
    time.tm_mday = day;
    time.tm_mon = month - 1;
    time.tm_year = year - 1900;
    time.tm_wday = 0;    // ignored for mktime
    time.tm_yday = 0;    // ignored for mktime
    time.tm_isdst = -1;  // no information available
    return std::mktime(&time);
  }

  const size_t _startdate = get_time(1992, 01, 01);
  const size_t _currentdate = get_time(1995, 06, 17);
  const size_t _enddate = get_time(1998, 12, 31);
  const size_t _one_day = get_time(1970, 01, 02) - get_time(1970, 01, 01);

  const std::vector<size_t> _region_keys_per_nation = {
    0, 1, 1,
    1, 4, 0,
    3, 3, 2,
    2, 4, 4,
    2, 4, 0,
    0, 0, 1,
    2, 3, 4,
    2, 3, 3,
    1};

  RandomGenerator _random_gen;
  TextFieldGenerator _text_field_gen;
};
}  // namespace tpch
