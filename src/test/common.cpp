#include "common.hpp"

#include <algorithm>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../lib/storage/table.hpp"
#include "../lib/types.hpp"

namespace opossum {

using Matrix = std::vector<std::vector<opossum::AllTypeVariant>>;

Matrix tableToMatrix(const opossum::Table &t) {
  Matrix matrix;

  // initialize matrix with table sizes
  matrix.resize(t.row_count(), std::vector<opossum::AllTypeVariant>(t.col_count()));

  // set values
  unsigned row_offset = 0;
  for (opossum::ChunkID chunk_id = 0; chunk_id < t.chunk_count(); chunk_id++) {
    const opossum::Chunk &chunk = t.get_chunk(chunk_id);

    for (size_t col_id = 0; col_id < t.col_count(); ++col_id) {
      std::shared_ptr<opossum::BaseColumn> column = chunk.get_column(col_id);

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset][col_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk.size();
  }

  return matrix;
}

void printMatrix(const std::vector<std::vector<opossum::AllTypeVariant>> &m) {
  std::cout << "-------------" << std::endl;
  for (unsigned row = 0; row < m.size(); row++) {
    for (unsigned col = 0; col < m[row].size(); col++) {
      std::cout << std::setw(8) << m[row][col] << " ";
    }
    std::cout << std::endl;
  }
  std::cout << "-------------" << std::endl;
}

::testing::AssertionResult tablesEqual(const opossum::Table &tleft, const opossum::Table &tright,
                                       bool order_sensitive) {
  Matrix left = tableToMatrix(tleft);
  Matrix right = tableToMatrix(tright);
  // compare schema of tables
  //  - column count
  if (tleft.col_count() != tright.col_count()) {
    printMatrix(left);
    printMatrix(right);
    return ::testing::AssertionFailure() << "Number of columns is different.";
  }
  //  - column names and types
  for (size_t col_id = 0; col_id < tright.col_count(); ++col_id) {
    if (tleft.column_type(col_id) != tright.column_type(col_id) ||
        tleft.column_name(col_id) != tright.column_name(col_id)) {
      return ::testing::AssertionFailure() << "Table schema is different.";
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (tleft.row_count() != tright.row_count()) {
    printMatrix(left);
    printMatrix(right);
    return ::testing::AssertionFailure() << "Number of rows is different.";
  }

  // sort if order does not matter
  if (!order_sensitive) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
  }

  if (left == right) {
    return ::testing::AssertionSuccess();
  } else {
    printMatrix(left);
    printMatrix(right);
    return ::testing::AssertionFailure() << "Table content is different.";
  }
}

template <typename T>
std::vector<T> split(std::string str, char delimiter) {
  std::vector<T> internal;
  std::stringstream ss(str);
  std::string tok;

  while (std::getline(ss, tok, delimiter)) {
    internal.push_back(tok);
  }

  return internal;
}

std::shared_ptr<opossum::Table> loadTable(std::string file_name, size_t chunk_size) {
  std::shared_ptr<opossum::Table> test_table = std::make_shared<opossum::Table>(opossum::Table(chunk_size));

  std::ifstream infile(file_name);
  std::string line;

  std::getline(infile, line);
  std::vector<std::string> col_names = split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> col_types = split<std::string>(line, '|');

  for (size_t i = 0; i < col_names.size(); i++) {
    test_table->add_column(col_names[i], col_types[i]);
  }

  while (std::getline(infile, line)) {
    std::vector<opossum::AllTypeVariant> token = split<opossum::AllTypeVariant>(line, '|');
    test_table->append(token);
  }
  return test_table;
}

}  // namespace opossum
