#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_operator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "storage/column_encoding_utils.hpp"
#include "storage/dictionary_column.hpp"
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

static std::string test_data_path;  // NOLINT

template <typename ParamType>
class BaseTestWithParam : public std::conditional<std::is_same<ParamType, void>::value, ::testing::Test,
                                                  ::testing::TestWithParam<ParamType>>::type {
 protected:
  // creates a dictionary column with the given type and values
  template <typename T>
  static std::shared_ptr<DictionaryColumn<T>> create_dict_column_by_type(DataType data_type,
                                                                         const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_column = std::make_shared<ValueColumn<T>>(std::move(vector_values));

    auto compressed_column = encode_column(EncodingType::Dictionary, data_type, value_column);
    return std::static_pointer_cast<DictionaryColumn<T>>(compressed_column);
  }

  void _execute_all(const std::vector<std::shared_ptr<AbstractOperator>>& operators) {
    for (auto& op : operators) {
      op->execute();
    }
  }

 public:
  BaseTestWithParam() {
#if HYRISE_NUMA_SUPPORT
    // Set options with very short cycle times
    auto options = NUMAPlacementManager::Options();
    options.counter_history_interval = std::chrono::milliseconds(1);
    options.counter_history_range = std::chrono::milliseconds(70);
    options.migration_interval = std::chrono::milliseconds(100);
    NUMAPlacementManager::get().set_options(options);
#endif
  }

  ~BaseTestWithParam() {
    // Reset scheduler first so that all tasks are done before we kill the StorageManager
    CurrentScheduler::set(nullptr);

#if HYRISE_NUMA_SUPPORT
    // Also make sure that the tasks in the NUMAPlacementManager are not running anymore. We don't restart it here.
    // If you want the NUMAPlacementManager in your test, start it yourself. This is to prevent migrations where we
    // don't expect them
    NUMAPlacementManager::get().pause();
#endif

    StorageManager::reset();
    TransactionManager::reset();
  }
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
