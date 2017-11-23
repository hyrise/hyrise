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

namespace {

using Matrix = std::vector<std::vector<opossum::AllTypeVariant>>;

Matrix _table_to_matrix(const std::shared_ptr<const opossum::Table>& t) {
  // initialize matrix with table sizes
  Matrix matrix(t->row_count(), std::vector<opossum::AllTypeVariant>(t->column_count()));

  // set values
  unsigned row_offset = 0;
  for (opossum::ChunkID chunk_id{0}; chunk_id < t->chunk_count(); chunk_id++) {
    const opossum::Chunk& chunk = t->get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk.size() == 0) continue;

    for (opossum::ColumnID col_id{0}; col_id < t->column_count(); ++col_id) {
      const auto column = chunk.get_column(col_id);

      for (opossum::ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset][col_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk.size();
  }

  return matrix;
}

std::string _matrix_to_string(const std::string& title, const Matrix& m) {
  std::stringstream stream;
  stream << "-------" << title << "-------" << std::endl;
  for (unsigned row = 0; row < m.size(); row++) {
    for (opossum::ColumnID col{0}; col < m[row].size(); col++) {
      stream << std::setw(8) << m[row][col] << " ";
    }
    stream << std::endl;
  }
  stream << "---------------------------" << std::endl;
  return stream.str();
}
}  // namespace

namespace opossum {

bool check_table_equal(const std::shared_ptr<const Table>& opossum_table,
                       const std::shared_ptr<const Table>& expected_table, OrderSensitivity order_sensitivity,
                       TypeCmpMode type_cmp_mode, FloatComparisonMode float_comparison_mode) {
  auto opossum_matrix = _table_to_matrix(opossum_table);
  auto expected_matrix = _table_to_matrix(expected_table);

  const auto generate_table_comparison = [&]() {
    std::stringstream stream;
    stream << "========= Tables are not equal =========" << std::endl;
    stream << _matrix_to_string("Actual Result", opossum_matrix);
    stream << std::endl;
    stream << _matrix_to_string("Expected Result", expected_matrix);
    stream << "========================================" << std::endl;
    return stream.str();
  };

  // compare schema of tables
  //  - column count
  if (opossum_table->column_count() != expected_table->column_count()) {
    std::cout << generate_table_comparison() << "Number of columns is different. " << std::endl
              << "Actual result: " << opossum_table->column_count()
              << ", Expected result: " << expected_table->column_count();
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

    if (left_col_type != right_col_type || opossum_table->column_name(col_id) != expected_table->column_name(col_id)) {
      const auto opossum_column_data_type = data_type_to_string.left.at(opossum_table->column_type(col_id));
      const auto expected_column_data_type = data_type_to_string.left.at(expected_table->column_type(col_id));

      std::cout << generate_table_comparison() << "Table schema is different."
                << "Column with ID " << col_id << " is different" << std::endl
                << "Got: " << opossum_table->column_name(col_id) << " (" << opossum_column_data_type << ")" << std::endl
                << "Expected: " << expected_table->column_name(col_id) << " (" << expected_column_data_type << ")"
                << std::endl;
      return false;
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (opossum_table->row_count() != expected_table->row_count()) {
    std::cout << "Number of rows is different." << std::endl
              << "Got: " << opossum_table->row_count() << " rows" << std::endl
              << "Expected: " << expected_table->row_count() << " rows" << generate_table_comparison() << std::endl;
    return false;
  }

  // sort if order does not matter
  if (order_sensitivity == OrderSensitivity::No) {
    std::sort(opossum_matrix.begin(), opossum_matrix.end());
    std::sort(expected_matrix.begin(), expected_matrix.end());
  }

  for (unsigned row = 0; row < opossum_matrix.size(); row++)
    for (ColumnID col{0}; col < opossum_matrix[row].size(); col++) {
      if (variant_is_null(opossum_matrix[row][col]) || variant_is_null(expected_matrix[row][col])) {
        EXPECT_TRUE(variant_is_null(opossum_matrix[row][col]) && variant_is_null(expected_matrix[row][col]));
      } else if (opossum_table->column_type(col) == DataType::Float) {
        auto left_val = type_cast<float>(opossum_matrix[row][col]);
        auto right_val = type_cast<float>(expected_matrix[row][col]);

        if (type_cmp_mode == TypeCmpMode::Strict) {
          EXPECT_EQ(expected_table->column_type(col), DataType::Float);
        } else {
          EXPECT_TRUE(expected_table->column_type(col) == DataType::Float ||
                      expected_table->column_type(col) == DataType::Double);
        }
        if (float_comparison_mode == FloatComparisonMode::AbsoluteDifference) {
          EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
        } else {
          EXPECT_REL_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
        }
      } else if (opossum_table->column_type(col) == DataType::Double) {
        auto left_val = type_cast<double>(opossum_matrix[row][col]);
        auto right_val = type_cast<double>(expected_matrix[row][col]);

        if (type_cmp_mode == TypeCmpMode::Strict) {
          EXPECT_EQ(expected_table->column_type(col), DataType::Double);
        } else {
          EXPECT_TRUE(expected_table->column_type(col) == DataType::Float ||
                      expected_table->column_type(col) == DataType::Double);
        }
        if (float_comparison_mode == FloatComparisonMode::AbsoluteDifference) {
          EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
        } else {
          EXPECT_REL_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
        }
      } else {
        if (type_cmp_mode == TypeCmpMode::Lenient &&
            (opossum_table->column_type(col) == DataType::Int || opossum_table->column_type(col) == DataType::Long)) {
          auto left_val = type_cast<int64_t>(opossum_matrix[row][col]);
          auto right_val = type_cast<int64_t>(expected_matrix[row][col]);
          EXPECT_EQ(left_val, right_val) << "Row:" << row + 1 << " Col:" << col + 1;
        } else {
          EXPECT_EQ(opossum_matrix[row][col], expected_matrix[row][col]) << "Row:" << row + 1 << " Col:" << col + 1;
        }
      }
    }

  if (::testing::Test::HasFailure()) {
    std::cout << generate_table_comparison();
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

}  // namespace opossum
