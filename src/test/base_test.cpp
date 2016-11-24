#include "base_test.hpp"

#include "../lib/storage/storage_manager.hpp"

namespace opossum {

BaseTest::~BaseTest() { StorageManager::reset(); }

}  // namespace opossum
