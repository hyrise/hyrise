#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../lib/storage/dictionary_compression.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/value_column.hpp"
#include "../lib/types.hpp"

#include "gtest/gtest.h"

namespace opossum {

class AbstractASTNode;
class Table;

using Matrix = std::vector<std::vector<AllTypeVariant>>;

class BaseTest : public ::testing::Test {
  using Matrix = std::vector<std::vector<AllTypeVariant>>;

  // helper functions for _table_equal
  static BaseTest::Matrix _table_to_matrix(const Table& t);
  static void _print_matrix(const BaseTest::Matrix& m);

  // helper function for load_table
  template <typename T>
  static std::vector<T> _split(const std::string& str, char delimiter);

 protected:
  // compares two tables with regard to the schema and content
  // but ignores the internal representation (chunk size, column type)
  static ::testing::AssertionResult _table_equal(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);

  // creates a opossum table based from a file
  static std::shared_ptr<Table> load_table(const std::string& file_name, size_t chunk_size);
  static void EXPECT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);
  static void ASSERT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);

  static void EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                              bool order_sensitive = false, bool strict_types = true);
  static void ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                              bool order_sensitive = false, bool strict_types = true);

  static void ASSERT_INNER_JOIN_NODE(const std::shared_ptr<AbstractASTNode>& node, ScanType scanType,
                                     ColumnID left_column_id, ColumnID right_column_id);

  static void ASSERT_CROSS_JOIN_NODE(const std::shared_ptr<AbstractASTNode>& node);

  // creates a dictionary column with the given type and values
  template <class T>
  static std::shared_ptr<BaseColumn> create_dict_column_by_type(const std::string& type, const std::vector<T>& values) {
    auto vector_values = tbb::concurrent_vector<T>(values.begin(), values.end());
    auto value_column = std::make_shared<ValueColumn<T>>(std::move(vector_values));
    return DictionaryCompression::compress_column(type, value_column);
  }

 public:
  virtual ~BaseTest();
};

}  // namespace opossum
