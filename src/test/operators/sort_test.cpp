#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class operators_sort : public ::testing::Test {
  virtual void SetUp() {
    _test_table = std::make_shared<opossum::Table>(opossum::Table(2));

    _test_table->add_column("a", "int");
    _test_table->add_column("b", "float");

    _test_table->append({12345, 458.7f});
    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});

    opossum::StorageManager::get().add_table("table_a", std::move(_test_table));

    _gt = std::make_shared<opossum::GetTable>("table_a");
  }

 public:
  std::shared_ptr<opossum::Table> _test_table;
  std::shared_ptr<opossum::GetTable> _gt;
};
}