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
::testing::AssertionResult compareTables(const opossum::Table &tleft, const opossum::Table &tright, bool sorted) {
  if (tleft.col_count() != tright.col_count()) {
    return ::testing::AssertionFailure() << "Number of columns is different.";
  }
  for (size_t col_id = 0; col_id < tright.col_count(); ++col_id) {
    if (tleft.column_type(col_id) != tright.column_type(col_id) ||
        tleft.column_name(col_id) != tright.column_name(col_id)) {
      return ::testing::AssertionFailure() << "Table schema is different.";
    }
  }

  if (tleft.row_count() != tright.row_count()) {
    return ::testing::AssertionFailure() << "Number of rows is different.";
  }
  // initialize tables with sizes
  std::vector<std::vector<opossum::AllTypeVariant>> left;
  std::vector<std::vector<opossum::AllTypeVariant>> right;

  left.resize(tleft.row_count(), std::vector<opossum::AllTypeVariant>(tleft.col_count()));
  right.resize(tright.row_count(), std::vector<opossum::AllTypeVariant>(tright.col_count()));

  // initialize table values
  for (opossum::ChunkID chunk_id = 0; chunk_id < tleft.chunk_count(); chunk_id++) {
    const opossum::Chunk &chunk = tleft.get_chunk(chunk_id);

    for (size_t col_id = 0; col_id < tleft.col_count(); ++col_id) {
      std::shared_ptr<opossum::BaseColumn> column = chunk.get_column(col_id);

      for (size_t row = 0; row < chunk.size(); ++row) {
        left[row][col_id] = (*column)[row];
      }
    }
  }

  for (size_t chunk_id = 0; chunk_id < tright.chunk_count(); chunk_id++) {
    const opossum::Chunk &chunk = tright.get_chunk(chunk_id);

    for (size_t col_id = 0; col_id < tright.col_count(); ++col_id) {
      std::shared_ptr<opossum::BaseColumn> column = chunk.get_column(col_id);

      for (size_t row = 0; row < chunk.size(); ++row) {
        right[row][col_id] = (*column)[row];
      }
    }
  }

  // sort if order matters
  if (!sorted) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
  }

  if (left == right) {
    return ::testing::AssertionSuccess();
  } else {
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
  std::vector<opossum::AllTypeVariant> token;

  std::getline(infile, line);
  std::vector<std::string> col_names = split<std::string>(line, '|');
  std::vector<std::string> col_types = split<std::string>(line, '|');

  for (size_t i = 0; i < col_names.size(); i++) {
    test_table->add_column(col_names[i], col_types[i]);
  }

  while (std::getline(infile, line)) {
    token = split<opossum::AllTypeVariant>(line, '|');
    // for (size_t j = 0; j < col_types.size(); j++) {
    //   if (col_types[j] == "int") {
    //     token_a.push_back(std::stoi(token[j]));
    //   } else if (col_types[j] == "long") {
    //     token_a.push_back(std::stol(token[j]));
    //   } else if (col_types[j] == "float") {
    //     token_a.push_back(std::stof(token[j]));
    //   } else if (col_types[j] == "double") {
    //     token_a.push_back(std::stod(token[j]));
    //   } else if (col_types[j] == "string") {
    //     token_a.push_back(token[j]);
    //   } else {
    //     return nullptr;
    //   }
    // }
    test_table->append(token);
  }
  return test_table;
}

}  // namespace opossum
