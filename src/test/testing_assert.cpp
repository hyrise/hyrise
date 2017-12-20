#include "testing_assert.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#define ANSI_COLOR_RED "\x1B[31m"
#define ANSI_COLOR_GREEN "\x1B[32m"
#define ANSI_COLOR_BG_RED "\x1B[41m"
#define ANSI_COLOR_BG_GREEN "\x1B[42m"
#define ANSI_COLOR_RESET "\x1B[0m"

#define EPSILON 0.0001

namespace {

using Matrix = std::vector<std::vector<opossum::AllTypeVariant>>;

Matrix _table_to_matrix(const std::shared_ptr<const opossum::Table>& t) {
  // initialize matrix with table sizes, including column names/types
  Matrix matrix(t->row_count() + 2, std::vector<opossum::AllTypeVariant>(t->column_count()));

  // set column names/types
  for (opossum::ColumnID col_id{0}; col_id < t->column_count(); ++col_id) {
    matrix[0][col_id] = t->column_name(col_id);
    matrix[1][col_id] = opossum::data_type_to_string.left.at(t->column_type(col_id));
  }

  // set values
  unsigned row_offset = 0;
  for (opossum::ChunkID chunk_id{0}; chunk_id < t->chunk_count(); chunk_id++) {
    const opossum::Chunk& chunk = t->get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk.size() == 0) continue;

    for (opossum::ColumnID col_id{0}; col_id < t->column_count(); ++col_id) {
      const auto column = chunk.get_column(col_id);

      for (opossum::ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset + 2][col_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk.size();
  }

  return matrix;
}

std::string _matrix_to_string(const Matrix& matrix, const std::vector<std::pair<uint64_t, uint16_t>>& highlight_cells,
                              const std::string& highlight_color) {
  std::stringstream stream;
  bool highlight;
  bool previous_row_highlighted = false;

  std::string highlight_color_bg = ANSI_COLOR_BG_RED;
  if (highlight_color == ANSI_COLOR_GREEN) {
    highlight_color_bg = ANSI_COLOR_BG_GREEN;
  }

  for (unsigned row = 0; row < matrix.size(); row++) {
    highlight = false;
    auto it = std::find_if(highlight_cells.begin(), highlight_cells.end(),
                           [&](const std::pair<uint64_t, uint16_t>& element) { return element.first == row; });
    if (it != highlight_cells.end()) {
      highlight = true;
      if (!previous_row_highlighted) {
        stream << "<<<<<" << std::endl;
        previous_row_highlighted = true;
      }
    } else {
      previous_row_highlighted = false;
    }

    // Highlight row number with background color
    std::string coloring = "";
    if (highlight) {
      coloring = highlight_color_bg;
    }
    if (row >= 2) {
      stream << coloring << std::setw(4) << std::to_string(row - 2) << ANSI_COLOR_RESET;
    } else {
      stream << coloring << std::setw(4) << "    " << ANSI_COLOR_RESET;
    }

    // Highlicht each (applicable) cell with highlight color
    for (opossum::ColumnID col{0}; col < matrix[row].size(); col++) {
      std::string cell = boost::lexical_cast<std::string>(matrix[row][col]);
      coloring = "";
      if (highlight && it->second == col) {
        coloring = highlight_color;
      }
      stream << coloring << std::setw(8) << cell << ANSI_COLOR_RESET << " ";
    }
    stream << std::endl;
  }
  return stream.str();
}

template <typename T, typename std::enable_if<std::is_floating_point<T>::value>::type* = nullptr>
bool near(T left_val, T right_val, opossum::FloatComparisonMode float_comparison_mode) {
  if (float_comparison_mode == opossum::FloatComparisonMode::AbsoluteDifference) {
    return std::fabs(left_val - right_val) < EPSILON;
  } else {
    return std::fabs(left_val - right_val) < std::fabs(right_val * EPSILON);
  }
}

}  // namespace

namespace opossum {

bool check_table_equal(const std::shared_ptr<const Table>& opossum_table,
                       const std::shared_ptr<const Table>& expected_table, OrderSensitivity order_sensitivity,
                       TypeCmpMode type_cmp_mode, FloatComparisonMode float_comparison_mode) {
  auto opossum_matrix = _table_to_matrix(opossum_table);
  auto expected_matrix = _table_to_matrix(expected_table);

  const auto generate_table_comparison = [&](const std::string& error_type, const std::string& error_msg,
                                             const std::vector<std::pair<uint64_t, uint16_t>>& highlighted_cells = {}) {
    std::stringstream stream;
    stream << "========= Tables are not equal =========" << std::endl;
    stream << "------- Actual Result -------" << std::endl;
    stream << _matrix_to_string(opossum_matrix, highlighted_cells, ANSI_COLOR_RED);
    stream << "-----------------------------" << std::endl << std::endl;
    stream << "------- Expected Result -------" << std::endl;
    stream << _matrix_to_string(expected_matrix, highlighted_cells, ANSI_COLOR_GREEN);
    stream << "-------------------------------" << std::endl;
    stream << "========================================" << std::endl << std::endl;
    stream << "Type of error: " << error_type << std::endl;
    stream << error_msg << std::endl;
    return stream.str();
  };

  // compare schema of tables
  //  - column count
  if (opossum_table->column_count() != expected_table->column_count()) {
    const std::string error_type = "Column count mismatch";
    const std::string error_msg = "Actual number of columns: " + std::to_string(opossum_table->column_count()) + "\n" +
                                  "Expected number of columns: " + std::to_string(expected_table->column_count());

    std::cout << generate_table_comparison(error_type, error_msg) << std::endl;
    return false;
  }

  //  - column names and types
  DataType left_col_type, right_col_type;
  for (ColumnID col_id{0}; col_id < expected_table->column_count(); ++col_id) {
    left_col_type = opossum_table->column_type(col_id);
    right_col_type = expected_table->column_type(col_id);
    // This is needed for the SQLiteTestrunner, since SQLite does not differentiate between float/double, and int/long.
    if (type_cmp_mode == TypeCmpMode::Lenient) {
      if (left_col_type == DataType::Double) {
        left_col_type = DataType::Float;
      } else if (left_col_type == DataType::Long) {
        left_col_type = DataType::Int;
      }

      if (right_col_type == DataType::Double) {
        right_col_type = DataType::Float;
      } else if (right_col_type == DataType::Long) {
        right_col_type = DataType::Int;
      }
    }

    if (opossum_table->column_name(col_id) != expected_table->column_name(col_id)) {
      const std::string error_type = "Column name mismatch (column " + std::to_string(col_id) + ")";
      const std::string error_msg = "Actual column name: " + opossum_table->column_name(col_id) + "\n" +
                                    "Expected column name: " + expected_table->column_name(col_id);

      std::cout << generate_table_comparison(error_type, error_msg, {{0, col_id}}) << std::endl;
      return false;
    }

    if (left_col_type != right_col_type) {
      const std::string error_type = "Column type mismatch (column " + std::to_string(col_id) + ")";
      const std::string error_msg =
          "Actual column type: " + data_type_to_string.left.at(opossum_table->column_type(col_id)) + "\n" +
          "Expected column type: " + data_type_to_string.left.at(expected_table->column_type(col_id));

      std::cout << generate_table_comparison(error_type, error_msg, {{1, col_id}}) << std::endl;
      return false;
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (opossum_table->row_count() != expected_table->row_count()) {
    const std::string error_type = "Row count mismatch";
    const std::string error_msg = "Actual number of rows: " + std::to_string(opossum_table->row_count()) + "\n" +
                                  "Expected number of rows: " + std::to_string(expected_table->row_count());

    std::cout << generate_table_comparison(error_type, error_msg) << std::endl;
    return false;
  }

  // sort if order does not matter
  if (order_sensitivity == OrderSensitivity::No) {
    // skip header when sorting
    std::sort(opossum_matrix.begin() + 2, opossum_matrix.end());
    std::sort(expected_matrix.begin() + 2, expected_matrix.end());
  }

  bool has_error = false;
  std::vector<std::pair<uint64_t, uint16_t>> mismatched_cells{};

  const auto expect_true = [&has_error, &mismatched_cells](bool statement, uint64_t row, uint16_t col) {
    if (!statement) {
      has_error = true;
      mismatched_cells.push_back({row, col});
    }
  };

  // Compare each cell, skipping header
  for (unsigned row = 2; row < opossum_matrix.size(); row++)
    for (ColumnID col{0}; col < opossum_matrix[row].size(); col++) {
      if (variant_is_null(opossum_matrix[row][col]) || variant_is_null(expected_matrix[row][col])) {
        expect_true(variant_is_null(opossum_matrix[row][col]) && variant_is_null(expected_matrix[row][col]), row, col);
      } else if (opossum_table->column_type(col) == DataType::Float) {
        auto left_val = type_cast<float>(opossum_matrix[row][col]);
        auto right_val = type_cast<float>(expected_matrix[row][col]);

        expect_true(near(left_val, right_val, float_comparison_mode), row, col);
      } else if (opossum_table->column_type(col) == DataType::Double) {
        auto left_val = type_cast<double>(opossum_matrix[row][col]);
        auto right_val = type_cast<double>(expected_matrix[row][col]);

        expect_true(near(left_val, right_val, float_comparison_mode), row, col);
      } else {
        if (type_cmp_mode == TypeCmpMode::Lenient &&
            (opossum_table->column_type(col) == DataType::Int || opossum_table->column_type(col) == DataType::Long)) {
          auto left_val = type_cast<int64_t>(opossum_matrix[row][col]);
          auto right_val = type_cast<int64_t>(expected_matrix[row][col]);
          expect_true(left_val == right_val, row, col);
        } else {
          expect_true(opossum_matrix[row][col] == expected_matrix[row][col], row, col);
        }
      }
    }

  if (has_error) {
    const std::string error_type = "Cell data mismatch";
    std::string error_msg = "Mismatched cells (row,column): ";
    for (auto cell : mismatched_cells) {
      error_msg += "(" + std::to_string(cell.first - 2) + "," + std::to_string(cell.second) + ") ";
    }

    std::cout << generate_table_comparison(error_type, error_msg, mismatched_cells) << std::endl;
    return false;
  }

  return true;
}

void ASSERT_INNER_JOIN_NODE(const std::shared_ptr<AbstractLQPNode>& node, ScanType scan_type, ColumnID left_column_id,
                            ColumnID right_column_id) {
  ASSERT_EQ(node->type(), LQPNodeType::Join);  // Can't cast otherwise
  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  ASSERT_EQ(join_node->join_mode(), JoinMode::Inner);  // Can't access join_column_ids() otherwise
  EXPECT_EQ(join_node->scan_type(), scan_type);
  EXPECT_EQ(join_node->join_column_ids(), std::make_pair(left_column_id, right_column_id));
}

void ASSERT_CROSS_JOIN_NODE(const std::shared_ptr<AbstractLQPNode>& node) {}

bool check_lqp_tie(const std::shared_ptr<const AbstractLQPNode>& parent, LQPChildSide child_side,
                   const std::shared_ptr<const AbstractLQPNode>& child) {
  auto parents = child->parents();
  for (const auto& parent2 : parents) {
    if (!parent2) {
      return false;
    }
    if (parent == parent2 && parent2->child(child_side) == child) {
      return true;
    }
  }

  return false;
}

bool subtree_types_are_equal(const std::shared_ptr<AbstractLQPNode>& got,
                             const std::shared_ptr<AbstractLQPNode>& expected) {
  if (got == nullptr && expected == nullptr) return true;
  if (got == nullptr) return false;
  if (expected == nullptr) return false;

  if (got->type() != expected->type()) return false;
  return subtree_types_are_equal(got->left_child(), expected->left_child()) &&
         subtree_types_are_equal(got->right_child(), expected->right_child());
}

}  // namespace opossum
