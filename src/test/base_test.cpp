#include "base_test.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "type_cast.hpp"
#include "utils/load_table.hpp"

namespace opossum {

BaseTest::~BaseTest() {
  StorageManager::reset();
  TransactionManager::reset();
}

}  // namespace opossum
