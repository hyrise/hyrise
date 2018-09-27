#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_operator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class AbstractLQPNode;
class Table;

extern std::string test_data_path;

template <typename ParamType>
class BaseTestWithParam
    : public std::conditional_t<std::is_same_v<ParamType, void>, ::testing::Test, ::testing::TestWithParam<ParamType>> {
 protected:
  // creates a dictionary segment with the given type and values
  template <typename T>
  static std::shared_ptr<DictionarySegment<T>> create_dict_segment_by_type(DataType data_type,
                                                                           const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_segment = std::make_shared<ValueSegment<T>>(std::move(vector_values));

    auto compressed_segment = encode_segment(EncodingType::Dictionary, data_type, value_segment);
    return std::static_pointer_cast<DictionarySegment<T>>(compressed_segment);
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

    PluginManager::reset();
    StorageManager::reset();
    TransactionManager::reset();
  }
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
