#include <iostream>

#include "operators/print.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/sqlite_wrapper.hpp"

using namespace opossum;  // NOLINT

int main() {
  SQLiteWrapper wrapper;
  wrapper.create_table(*load_table("resources/test_data/tbl/int_int_int.tbl"), "t");
  Print::print(wrapper.execute_query("SELECT * FROM t"));

  return 0;
}
