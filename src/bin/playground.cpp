#include <iostream>
#include <memory>

#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "../benchmark/tpcc/tpcc_table_generator.hpp"

using namespace opossum;

int main() {
  TPCCTableGenerator generator;

  generator.add_all_tables(StorageManager::get());

  StorageManager::get().dump_as_csv(".");

  return 0;
}
