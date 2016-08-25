#include "table_scan.hpp"

#include <memory>
#include <string>

namespace opossum {

table_scan::table_scan(const std::shared_ptr<abstract_operator> in, const std::string &column_name,
                       /* const std::string &op*/ const all_type_variant value)
    : abstract_operator(in),
      _impl(make_unique_templated<abstract_operator_impl, table_scan_impl>(
          _input_left->get_column_type(_input_left->get_column_id_by_name(column_name)), in, column_name,
          /*op,*/ value)) {}

const std::string table_scan::get_name() const { return "table_scan"; }

uint8_t table_scan::get_num_in_tables() const { return 1; }

uint8_t table_scan::get_num_out_tables() const { return 1; }

void table_scan::execute() { _impl->execute(); }

std::shared_ptr<table> table_scan::get_output() const { return _impl->get_output(); }
}  // namespace opossum
