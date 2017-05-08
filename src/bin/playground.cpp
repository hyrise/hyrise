#include <iostream>
#include <memory>

#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "../benchmark/tpcc/tpcc_table_generator.hpp"

using namespace opossum;

void save_as_csv(const std::shared_ptr<Table> & table, const std::string & path)
{
  auto tableWrapper = std::make_shared<TableWrapper>(table);
  tableWrapper->execute();

  auto exportCsv = std::make_shared<ExportCsv>(tableWrapper, ".", path);
  exportCsv->execute();
}

int main() {
  TPCCTableGenerator generator;

  save_as_csv(generator.generate_items_table(), "tpcc-items.csv");



}
