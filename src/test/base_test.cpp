#include "base_test.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "type_cast.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void BaseTest::EXPECT_TABLE_EQ(const Table &tleft, const Table &tright, bool order_sensitive) {
  EXPECT_TRUE(_table_equal(tleft, tright, order_sensitive));
}

void BaseTest::ASSERT_TABLE_EQ(const Table &tleft, const Table &tright, bool order_sensitive) {
  ASSERT_TRUE(_table_equal(tleft, tright, order_sensitive));
}

void BaseTest::EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                               bool order_sensitive) {
  EXPECT_TABLE_EQ(*tleft, *tright, order_sensitive);
}

void BaseTest::ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                               bool order_sensitive) {
  ASSERT_TABLE_EQ(*tleft, *tright, order_sensitive);
}

BaseTest::Matrix BaseTest::_table_to_matrix(const Table &t) {
  // initialize matrix with table sizes
  Matrix matrix(t.row_count(), std::vector<AllTypeVariant>(t.col_count()));

  // set values
  unsigned row_offset = 0;
  for (ChunkID chunk_id{0}; chunk_id < t.chunk_count(); chunk_id++) {
    const Chunk &chunk = t.get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk.size() == 0) continue;

    for (ColumnID col_id{0}; col_id < t.col_count(); ++col_id) {
      std::shared_ptr<BaseColumn> column = chunk.get_column(col_id);

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset][col_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk.size();
  }

  return matrix;
}

void BaseTest::_print_matrix(const BaseTest::Matrix &m) {
  std::cout << "-------------" << std::endl;
  for (unsigned row = 0; row < m.size(); row++) {
    for (ColumnID col{0}; col < m[row].size(); col++) {
      std::cout << std::setw(8) << m[row][col] << " ";
    }
    std::cout << std::endl;
  }
  std::cout << "-------------" << std::endl;
}

::testing::AssertionResult BaseTest::_table_equal(const Table &tleft, const Table &tright, bool order_sensitive) {
  Matrix left = _table_to_matrix(tleft);
  Matrix right = _table_to_matrix(tright);
  // compare schema of tables
  //  - column count
  if (tleft.col_count() != tright.col_count()) {
    _print_matrix(left);
    _print_matrix(right);
    return ::testing::AssertionFailure() << "Number of columns is different.";
  }
  //  - column names and types
  for (ColumnID col_id{0}; col_id < tright.col_count(); ++col_id) {
    if (tleft.column_type(col_id) != tright.column_type(col_id) ||
        tleft.column_name(col_id) != tright.column_name(col_id)) {
      std::cout << "Column with ID " << col_id << " is different" << std::endl;
      std::cout << "Got: " << tleft.column_name(col_id) << " (" << tleft.column_type(col_id) << ")" << std::endl;
      std::cout << "Expected: " << tright.column_name(col_id) << " (" << tright.column_type(col_id) << ")" << std::endl;
      return ::testing::AssertionFailure() << "Table schema is different.";
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (tleft.row_count() != tright.row_count()) {
    _print_matrix(left);
    _print_matrix(right);
    std::cout << "Got: " << tleft.row_count() << " rows" << std::endl;
    std::cout << "Expected: " << tright.row_count() << " rows" << std::endl;
    return ::testing::AssertionFailure() << "Number of rows is different.";
  }

  // sort if order does not matter
  if (!order_sensitive) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
  }

  for (unsigned row = 0; row < left.size(); row++)
    for (ColumnID col{0}; col < left[row].size(); col++) {
      if (is_null(left[row][col]) || is_null(right[row][col])) {
        EXPECT_TRUE(is_null(left[row][col]) && is_null(right[row][col]));
      } else if (tleft.column_type(col) == "float") {
        auto left_val = type_cast<float>(left[row][col]);
        auto right_val = type_cast<float>(right[row][col]);

        EXPECT_EQ(tright.column_type(col), "float");
        EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
      } else if (tleft.column_type(col) == "double") {
        auto left_val = type_cast<double>(left[row][col]);
        auto right_val = type_cast<double>(right[row][col]);

        EXPECT_EQ(tright.column_type(col), "double");
        EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
      } else {
        EXPECT_EQ(left[row][col], right[row][col]) << "Row:" << row + 1 << " Col:" << col + 1;
      }
    }

  return ::testing::AssertionSuccess();
}

std::shared_ptr<Table> BaseTest::load_table(const std::string &file_name, size_t chunk_size) {
  return opossum::load_table(file_name, chunk_size);
}

BaseTest::~BaseTest() {
  StorageManager::reset();
  TransactionManager::reset();
}

}  // namespace opossum
