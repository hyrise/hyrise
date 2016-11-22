#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../lib/storage/table.hpp"
#include "../lib/types.hpp"
#include "gtest/gtest.h"

namespace opossum {

using Matrix = std::vector<std::vector<AllTypeVariant>>;

class BaseTest : public ::testing::Test {
  virtual void TearDown();

  // helper functions for tablesEqual
  static Matrix tableToMatrix(const Table &t);
  static void printMatrix(const std::vector<std::vector<AllTypeVariant>> &m);

  // helper function for loadTable
  template <typename T>
  static std::vector<T> split(std::string str, char delimiter);

 protected:
  // compares two tables with regard to the schema and content
  // but ignores the internal representation (chunk size, column type)
  static ::testing::AssertionResult tablesEqual(const Table &tleft, const Table &tright, bool order_sensitive = false);

  // creates a opossum table based from a file
  static std::shared_ptr<Table> loadTable(std::string file_name, size_t chunk_size);
};

}  // namespace opossum
