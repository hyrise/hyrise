#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <utility>

#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/nested_loop_join.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

/*
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
*/

std::random_device rd;
std::mt19937 eng(rd());
std::uniform_int_distribution<> distr(0, 99);

int random_int() { return distr(eng); }

double random_double() {
  int a = distr(eng);
  int b = distr(eng);
  return static_cast<double>(a) + static_cast<double>(b) / 100.0;
}

float random_float() { return static_cast<float>(random_double()); }

int main() {
  auto t1 = std::make_shared<opossum::Table>(opossum::Table(100));
  auto t2 = std::make_shared<opossum::Table>(opossum::Table(100));

  t1->add_column("a", "int");
  t1->add_column("b", "float");
  t1->add_column("d", "double");

  t2->add_column("a", "int");
  t2->add_column("b", "float");
  t2->add_column("d", "double");

  for (int i = 0; i < 10000; i++) {
    t1->append({random_int(), random_float(), random_double()});
    t2->append({random_int(), random_float(), random_double()});
  }

  opossum::StorageManager::get().add_table("table1", std::move(t1));
  opossum::StorageManager::get().add_table("table2", std::move(t2));

  auto gt1 = std::make_shared<opossum::GetTable>("table1");
  gt1->execute();

  auto gt2 = std::make_shared<opossum::GetTable>("table2");
  gt2->execute();

  auto s = std::make_shared<opossum::NestedLoopJoin>(gt1, gt2, std::pair<std::string, std::string>("a", "a"), "=",
                                                     opossum::JoinMode::Inner);
  auto start = std::chrono::steady_clock::now();
  s->execute();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
  std::cout << "duration: " << duration.count() << "ms" << std::endl;

  // opossum::StorageManager::get().add_table("result", s->get_output());

  // opossum::StorageManager::get().print();
}
