#include <cassert>
#include <memory>
#include <utility>

#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

int main() {
  auto t = std::make_shared<opossum::Table>(opossum::Table(2));

  t->add_column("a", "int");
  t->add_column("langer spaltenname", "float");
  t->add_column("c", "string");
  t->add_column("d", "double");

  t->append({123, 456.7, "testa", 51});
  t->append({1234, 457.7, "testb", 516.2});
  t->append({12345, 458.7, "testc", 62});

  opossum::StorageManager::get().add_table("meine_erste_tabelle", std::move(t));

  auto gt = std::make_shared<opossum::GetTable>("meine_erste_tabelle");
  gt->execute();

  auto s = std::make_shared<opossum::TableScan>(gt, "a", ">=", 1234);
  s->execute();

  auto p = std::make_shared<opossum::Print>(s);
  p->execute();

  // omg - we can even SELECT INTO:
  opossum::StorageManager::get().add_table("meine_zweite_tabelle", p->get_output());

  opossum::StorageManager::get().print();
}
