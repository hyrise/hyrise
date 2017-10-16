#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/dictionary_compression.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "utils/load_table.hpp"
#include "types.hpp"

#include "testing_assert.hpp"

#include "gtest/gtest.h"

namespace opossum {

class AbstractASTNode;
class Table;

class BaseTest : public ::testing::Test {
 protected:
  // creates a dictionary column with the given type and values
  template <class T>
  static std::shared_ptr<BaseColumn> create_dict_column_by_type(const std::string& type, const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_column = std::make_shared<ValueColumn<T>>(std::move(vector_values));
    return DictionaryCompression::compress_column(type, value_column);
  }

 public:
  ~BaseTest() override;
};

}  // namespace opossum
