#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_operator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class AbstractLQPNode;
class Table;

template <typename ParamType>
class BaseTestWithParam : public std::conditional<std::is_same<ParamType, void>::value, ::testing::Test,
                                                  ::testing::TestWithParam<ParamType>>::type {
 protected:
  // creates a dictionary column with the given type and values
  template <class T>
  static std::shared_ptr<DictionaryColumn<T>> create_dict_column_by_type(DataType data_type,
                                                                         const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_column = std::make_shared<ValueColumn<T>>(std::move(vector_values));
    auto compressed_column = DictionaryCompression::compress_column(data_type, value_column);
    return std::static_pointer_cast<DictionaryColumn<T>>(compressed_column);
  }

  void _execute_all(const std::vector<std::shared_ptr<AbstractOperator>>& operators) {
    for (auto& op : operators) {
      op->execute();
    }
  }

 public:
  BaseTestWithParam() {
    // Set options with very short cycle times
    auto options = NUMAPlacementManager::Options();
    options.counter_history_interval = std::chrono::milliseconds(1);
    options.counter_history_range = std::chrono::milliseconds(70);
    options.migration_interval = std::chrono::milliseconds(100);
    NUMAPlacementManager::get().set_options(options);
  }

  ~BaseTestWithParam() {
    // Reset scheduler first so that all tasks are done before we kill the StorageManager
    CurrentScheduler::set(nullptr);

    // Also make sure that the tasks in the NUMAPlacementManager are not running anymore. We don't restart it here.
    // If you want the NUMAPlacementManager in your test, start it yourself. This is to prevent migrations where we
    // don't expect them
    NUMAPlacementManager::get().pause();

    StorageManager::reset();
    TransactionManager::reset();
  }
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
