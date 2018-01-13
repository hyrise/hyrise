#include <iostream>

#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"

using namespace opossum;

int main() {
  StorageManager::get().add_table("int_int_int_100", load_table("src/test/tables/sqlite/int_int_int_100.tbl", 20));

  auto get_table = std::make_shared<GetTable>("int_int_int_100");
  auto table_scan_a = std::make_shared<TableScan>(get_table, ColumnID{0}, ScanType::GreaterThan, 20);
  auto table_scan_b = std::make_shared<TableScan>(get_table, ColumnID{1}, ScanType::GreaterThan, 30);
  auto union_positions = std::make_shared<UnionPositions>(table_scan_a, table_scan_b);

  for (auto& op : std::vector<std::shared_ptr<AbstractOperator>>({get_table, table_scan_a, table_scan_b, union_positions})) {
    op->execute();
  }

  get_table->execute();
  table_scan_a->execute();
  get_table->execute();
  get_table->execute();

  union_positions->print();

  return 0;
}
