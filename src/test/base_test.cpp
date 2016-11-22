#include "base_test.hpp"

#include "../lib/storage/storage_manager.hpp"

namespace opossum {

void BaseTest::TearDown() { StorageManager::reset(); }

}  // namespace opossum
