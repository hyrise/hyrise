#include "testing_assert.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "logical_query_plan/abstract_logical_query_plan_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace {

using Matrix = std::vector<std::vector<opossum::AllTypeVariant>>;

Matrix _table_to_matrix(const opossum::Table& t) {
  // initialize matrix with table sizes
  Matrix matrix(t.row_count(), std::vector<opossum::AllTypeVariant>(t.column_count()));

  // set values
  unsigned row_offset = 0;
  for (opossum::ChunkID chunk_id{0}; chunk_id < t.chunk_count(); chunk_id++) {
    const opossum::Chunk& chunk = t.get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk.size() == 0) continue;

    for (opossum::ColumnID col_id{0}; col_id < t.column_count(); ++col_id) {
      const auto column = chunk.get_column(col_id);

      for (opossum::ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset][col_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk.size();
  }

  return matrix;
}

void _print_matrix(const Matrix& m) {
  std::cout << "-------------" << std::endl;
  for (unsigned row = 0; row < m.size(); row++) {
    for (opossum::ColumnID col{0}; col < m[row].size(); col++) {
      std::cout << std::setw(8) << m[row][col] << " ";
    }
    std::cout << std::endl;
  }
  std::cout << "-------------" << std::endl;
}
}  // namespace

namespace opossum {

::testing::AssertionResult check_table_equal(const Table& tleft, const Table& tright, bool order_sensitive,
                                             bool strict_types) {
  Matrix left = _table_to_matrix(tleft);
  Matrix right = _table_to_matrix(tright);

  const auto print_tables = [&]() {
    std::cout << "== Tables are not equal ==" << std::endl;
    _print_matrix(left);
    _print_matrix(right);
    std::cout << "==========================" << std::endl;
  };

  // compare schema of tables
  //  - column count
  if (tleft.column_count() != tright.column_count()) {
    print_tables();
    return ::testing::AssertionFailure() << "Number of columns is different. " << tleft.column_count()
                                         << " != " << tright.column_count();
  }

  //  - column names and types
  std::string left_col_type, right_col_type;
  for (ColumnID col_id{0}; col_id < tright.column_count(); ++col_id) {
    left_col_type = tleft.column_type(col_id);
    right_col_type = tright.column_type(col_id);
    // This is needed for the SQLiteTestrunner, since SQLite does not differentiate between float/double, and int/long.
    if (!strict_types) {
      if (left_col_type == "double") {
        left_col_type = "float";
      } else if (left_col_type == "long") {
        left_col_type = "int";
      }

      if (right_col_type == "double") {
        right_col_type = "float";
      } else if (right_col_type == "long") {
        right_col_type = "int";
      }
    }
    if (left_col_type != right_col_type || tleft.column_name(col_id) != tright.column_name(col_id)) {
      std::cout << "Column with ID " << col_id << " is different" << std::endl;
      std::cout << "Got: " << tleft.column_name(col_id) << " (" << tleft.column_type(col_id) << ")" << std::endl;
      std::cout << "Expected: " << tright.column_name(col_id) << " (" << tright.column_type(col_id) << ")" << std::endl;
      print_tables();
      return ::testing::AssertionFailure() << "Table schema is different.";
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (tleft.row_count() != tright.row_count()) {
    std::cout << "Got: " << tleft.row_count() << " rows" << std::endl;
    std::cout << "Expected: " << tright.row_count() << " rows" << std::endl;
    print_tables();
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

        if (strict_types) {
          EXPECT_EQ(tright.column_type(col), "float");
        } else {
          EXPECT_TRUE(tright.column_type(col) == "float" || tright.column_type(col) == "double");
        }
        EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
      } else if (tleft.column_type(col) == "double") {
        auto left_val = type_cast<double>(left[row][col]);
        auto right_val = type_cast<double>(right[row][col]);

        if (strict_types) {
          EXPECT_EQ(tright.column_type(col), "double");
        } else {
          EXPECT_TRUE(tright.column_type(col) == "float" || tright.column_type(col) == "double");
        }
        EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
      } else {
        if (!strict_types && (tleft.column_type(col) == "int" || tleft.column_type(col) == "long")) {
          auto left_val = type_cast<int64_t>(left[row][col]);
          auto right_val = type_cast<int64_t>(right[row][col]);
          EXPECT_EQ(left_val, right_val) << "Row:" << row + 1 << " Col:" << col + 1;
        } else {
          EXPECT_EQ(left[row][col], right[row][col]) << "Row:" << row + 1 << " Col:" << col + 1;
        }
      }
    }

  if (::testing::Test::HasFailure()) {
    print_tables();
  }

  return ::testing::AssertionSuccess();
}

void EXPECT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive, bool strict_types) {
  EXPECT_TRUE(check_table_equal(tleft, tright, order_sensitive, strict_types));
}

void ASSERT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive, bool strict_types) {
  ASSERT_TRUE(check_table_equal(tleft, tright, order_sensitive, strict_types));
}

void EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright, bool order_sensitive,
                     bool strict_types) {
  EXPECT_TABLE_EQ(*tleft, *tright, order_sensitive, strict_types);
}

void ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright, bool order_sensitive,
                     bool strict_types) {
  ASSERT_TABLE_EQ(*tleft, *tright, order_sensitive, strict_types);
}

void ASSERT_INNER_JOIN_NODE(const std::shared_ptr<AbstractLogicalQueryPlanNode>& node, ScanType scanType, ColumnID left_column_id,
                            ColumnID right_column_id) {
  ASSERT_EQ(node->type(), LQPNodeType::Join);  // Can't cast otherwise
  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  ASSERT_EQ(join_node->join_mode(), JoinMode::Inner);  // Can't access join_column_ids() otherwise
  EXPECT_EQ(join_node->scan_type(), ScanType::OpEquals);
  EXPECT_EQ(join_node->join_column_ids(), std::make_pair(left_column_id, right_column_id));
}

void ASSERT_CROSS_JOIN_NODE(const std::shared_ptr<AbstractLogicalQueryPlanNode>& node) {}

bool check_lqp_tie(const std::shared_ptr<const AbstractLogicalQueryPlanNode>& parent, LQPChildSide child_side,
                   const std::shared_ptr<const AbstractLogicalQueryPlanNode>& child) {
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
