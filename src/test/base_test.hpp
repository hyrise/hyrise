#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_operator.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class AbstractASTNode;
class Table;

template <typename ParamType>
class BaseTestWithParam : public std::conditional<std::is_same<ParamType, void>::value, ::testing::Test,
                                                  ::testing::TestWithParam<ParamType>>::type {
 protected:
  // creates a dictionary column with the given type and values
  template <class T>
  static std::shared_ptr<DictionaryColumn<T>> create_dict_column_by_type(const std::string& type,
                                                                         const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_column = std::make_shared<ValueColumn<T>>(std::move(vector_values));
    auto compressed_column = DictionaryCompression::compress_column(type, value_column);
    return std::static_pointer_cast<DictionaryColumn<T>>(compressed_column);
  }

  void _execute_all(const std::vector<std::shared_ptr<AbstractOperator>>& operators) {
    for (auto& op : operators) {
      op->execute();
    }
  }

 public:
  ~BaseTestWithParam() override {
    StorageManager::reset();
    TransactionManager::reset();
  }
};

using BaseTest = BaseTestWithParam<void>;

}  // namespace opossum
