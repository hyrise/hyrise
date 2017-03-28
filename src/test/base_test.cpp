#include "base_test.hpp"

#include <algorithm>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../lib/concurrency/transaction_manager.hpp"
#include "../lib/storage/storage_manager.hpp"

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
  for (ChunkID chunk_id = 0; chunk_id < t.chunk_count(); chunk_id++) {
    const Chunk &chunk = t.get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk.size() == 0) continue;

    for (size_t col_id = 0; col_id < t.col_count(); ++col_id) {
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
    for (unsigned col = 0; col < m[row].size(); col++) {
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
  for (size_t col_id = 0; col_id < tright.col_count(); ++col_id) {
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
    return ::testing::AssertionFailure() << "Number of rows is different.";
  }

  // sort if order does not matter
  if (!order_sensitive) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
  }

  for (unsigned row = 0; row < left.size(); row++)
    for (unsigned col = 0; col < left[row].size(); col++) {
      if (tleft.column_type(col) == "float") {
        EXPECT_EQ(tright.column_type(col), "float");
        EXPECT_NEAR(type_cast<float>(left[row][col]), type_cast<float>(right[row][col]), 0.0001) << "Row/Col:" << row
                                                                                                 << "/" << col;
      } else if (tleft.column_type(col) == "double") {
        EXPECT_EQ(tright.column_type(col), "double");
        EXPECT_NEAR(type_cast<double>(left[row][col]), type_cast<double>(right[row][col]), 0.0001) << "Row/Col:" << row
                                                                                                   << "/" << col;
      } else {
        EXPECT_EQ(left[row][col], right[row][col]) << "Row:" << row + 1 << " Col:" << col + 1;
      }
    }

  return ::testing::AssertionSuccess();
}

template <typename T>
std::vector<T> BaseTest::_split(const std::string &str, char delimiter) {
  std::vector<T> internal;
  std::stringstream ss(str);
  std::string tok;

  while (std::getline(ss, tok, delimiter)) {
    internal.push_back(tok);
  }

  return internal;
}

std::shared_ptr<Table> BaseTest::load_table(const std::string &file_name, size_t chunk_size) {
  std::shared_ptr<Table> test_table = std::make_shared<Table>(chunk_size);

  std::ifstream infile(file_name);
  std::string line;

  std::getline(infile, line);
  std::vector<std::string> col_names = _split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> col_types = _split<std::string>(line, '|');

  for (size_t i = 0; i < col_names.size(); i++) {
    test_table->add_column(col_names[i], col_types[i]);
  }

  while (std::getline(infile, line)) {
    std::vector<AllTypeVariant> values = _split<AllTypeVariant>(line, '|');

    test_table->append(values);

    auto &chunk = test_table->get_chunk(test_table->chunk_count() - 1);
    auto &mvcc_cols = chunk.mvcc_columns();
    mvcc_cols.begin_cids.back() = 0;
  }
  return test_table;
}

BaseTest::~BaseTest() {
  StorageManager::reset();
  TransactionManager::reset();
}

}  // namespace opossum
