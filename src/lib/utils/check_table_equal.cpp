#include "check_table_equal.hpp"

#include <iomanip>
#include <iostream>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "lossless_cast.hpp"

static constexpr auto ANSI_COLOR_RED = "\x1B[31m";
static constexpr auto ANSI_COLOR_GREEN = "\x1B[32m";
static constexpr auto ANSI_COLOR_BG_RED = "\x1B[41m";
static constexpr auto ANSI_COLOR_BG_GREEN = "\x1B[42m";
static constexpr auto ANSI_COLOR_RESET = "\x1B[0m";

static constexpr auto EPSILON = 0.0001;

namespace {

using namespace opossum;  // NOLINT

constexpr int HEADER_SIZE = 3;

using Matrix = std::vector<std::vector<AllTypeVariant>>;

Matrix table_to_matrix(const std::shared_ptr<const Table>& table) {
  // initialize matrix with table sizes, including column names/types
  Matrix header(HEADER_SIZE, std::vector<AllTypeVariant>(table->column_count()));

  // set column names/types
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    header[0][column_id] = pmr_string{table->column_name(column_id)};
    header[1][column_id] = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};
    header[2][column_id] = pmr_string{table->column_is_nullable(column_id) ? "NULL" : "NOT NULL"};
  }

  // set values
  auto matrix = table->get_rows();
  matrix.insert(matrix.begin(), header.begin(), header.end());

  return matrix;
}

std::string matrix_to_string(const Matrix& matrix, const std::vector<std::pair<uint64_t, uint16_t>>& highlight_cells,
                             const std::string& highlight_color, const std::string& highlight_color_bg) {
  std::stringstream stream;
  bool previous_row_highlighted = false;

  for (auto row_id = size_t{0}; row_id < matrix.size(); row_id++) {
    auto highlight = false;
    auto it = std::find_if(highlight_cells.begin(), highlight_cells.end(),
                           [&](const auto& element) { return element.first == row_id; });
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
    auto coloring = std::string{};
    if (highlight) {
      coloring = highlight_color_bg;
    }
    if (row_id >= HEADER_SIZE) {
      stream << coloring << std::setw(4) << std::to_string(row_id - HEADER_SIZE) << ANSI_COLOR_RESET;
    } else {
      stream << coloring << std::setw(4) << "    " << ANSI_COLOR_RESET;
    }

    // Highlicht each (applicable) cell with highlight color
    for (auto column_id = ColumnID{0}; column_id < matrix[row_id].size(); column_id++) {
      auto cell = boost::lexical_cast<std::string>(matrix[row_id][column_id]);
      coloring = "";
      if (highlight && it->second == column_id) {
        coloring = highlight_color;
      }
      stream << coloring << std::setw(8) << cell << ANSI_COLOR_RESET << " ";
    }
    stream << std::endl;
  }
  return stream.str();
}

template <typename T>
bool almost_equals(T left_val, T right_val, FloatComparisonMode float_comparison_mode) {
  static_assert(std::is_floating_point_v<T>, "Values must be of floating point type.");
  if (float_comparison_mode == FloatComparisonMode::AbsoluteDifference) {
    return std::fabs(left_val - right_val) < EPSILON;
  } else {
    return std::fabs(left_val - right_val) < std::max(EPSILON, std::fabs(right_val * EPSILON));
  }
}

}  // namespace

namespace opossum {

bool check_segment_equal(const std::shared_ptr<BaseSegment>& actual_segment,
                         const std::shared_ptr<BaseSegment>& expected_segment, OrderSensitivity order_sensitivity,
                         TypeCmpMode type_cmp_mode, FloatComparisonMode float_comparison_mode) {
  if (actual_segment->data_type() != expected_segment->data_type()) {
    return false;
  }

  const auto definitions =
      std::vector<TableColumnDefinition>{TableColumnDefinition("single_column", actual_segment->data_type(), true)};

  auto table_type = [&](const std::shared_ptr<BaseSegment>& segment) {
    if (const auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(segment)) {
      return TableType::References;
    }
    return TableType::Data;
  };

  auto actual_table = std::make_shared<Table>(definitions, table_type(actual_segment));
  actual_table->append_chunk(pmr_vector<std::shared_ptr<BaseSegment>>{actual_segment});
  auto expected_table = std::make_shared<Table>(definitions, table_type(expected_segment));
  expected_table->append_chunk(pmr_vector<std::shared_ptr<BaseSegment>>{expected_segment});

  // If check_table_equal returns something other than std::nullopt, a difference has been found.
  return !check_table_equal(actual_table, expected_table, order_sensitivity, type_cmp_mode, float_comparison_mode,
                            IgnoreNullable::Yes);
}

std::optional<std::string> check_table_equal(const std::shared_ptr<const Table>& actual_table,
                                             const std::shared_ptr<const Table>& expected_table,
                                             OrderSensitivity order_sensitivity, TypeCmpMode type_cmp_mode,
                                             FloatComparisonMode float_comparison_mode,
                                             IgnoreNullable ignore_nullable) {
  if (!actual_table && expected_table) return "No 'actual' table given";
  if (actual_table && !expected_table) return "No 'expected' table given";
  if (!actual_table && !expected_table) return "No 'expected' table and no 'actual' table given";

  auto stream = std::stringstream{};

  auto actual_matrix = table_to_matrix(actual_table);
  auto expected_matrix = table_to_matrix(expected_table);

  // sort if order does not matter
  if (order_sensitivity == OrderSensitivity::No) {
    // skip header when sorting
    std::sort(actual_matrix.begin() + HEADER_SIZE, actual_matrix.end());
    std::sort(expected_matrix.begin() + HEADER_SIZE, expected_matrix.end());
  }

  const auto print_table_comparison = [&](const std::string& error_type, const std::string& error_msg,
                                          const std::vector<std::pair<uint64_t, uint16_t>>& highlighted_cells = {}) {
    stream << "===================== Tables are not equal =====================" << std::endl;
    stream << "------------------------- Actual Result ------------------------" << std::endl;
    stream << matrix_to_string(actual_matrix, highlighted_cells, ANSI_COLOR_RED, ANSI_COLOR_BG_RED);
    stream << "----------------------------------------------------------------" << std::endl << std::endl;
    stream << "------------------------ Expected Result -----------------------" << std::endl;
    stream << matrix_to_string(expected_matrix, highlighted_cells, ANSI_COLOR_GREEN, ANSI_COLOR_BG_GREEN);
    stream << "----------------------------------------------------------------" << std::endl;
    stream << "Type of error: " << error_type << std::endl;
    stream << "================================================================" << std::endl << std::endl;
    stream << error_msg << std::endl << std::endl;
  };

  // compare schema of tables
  //  - column count
  if (actual_table->column_count() != expected_table->column_count()) {
    const std::string error_type = "Column count mismatch";
    const std::string error_msg = "Actual number of columns: " + std::to_string(actual_table->column_count()) + "\n" +
                                  "Expected number of columns: " + std::to_string(expected_table->column_count());

    print_table_comparison(error_type, error_msg);
    return stream.str();
  }

  //  - column names and types
  for (auto column_id = ColumnID{0}; column_id < expected_table->column_count(); ++column_id) {
    auto actual_column_type = actual_table->column_data_type(column_id);
    const auto actual_column_is_nullable = actual_table->column_is_nullable(column_id);
    auto expected_column_type = expected_table->column_data_type(column_id);
    const auto expected_column_is_nullable = expected_table->column_is_nullable(column_id);
    // This is needed for the SQLiteTestrunner, since SQLite does not differentiate between float/double, and int/long.
    // Also, as sqlite returns a generic type for all-NULL columns, we need to adapt those columns to our type.
    if (type_cmp_mode == TypeCmpMode::Lenient) {
      if (actual_column_type == DataType::Double) {
        actual_column_type = DataType::Float;
      } else if (actual_column_type == DataType::Long) {
        actual_column_type = DataType::Int;
      }

      if (expected_column_type == DataType::Double) {
        expected_column_type = DataType::Float;
      } else if (expected_column_type == DataType::Long) {
        expected_column_type = DataType::Int;
      }

      bool all_null = true;
      for (auto row_id = size_t{HEADER_SIZE}; row_id < expected_matrix.size(); row_id++) {
        if (!variant_is_null(expected_matrix[row_id][column_id])) {
          all_null = false;
          break;
        }
      }
      if (all_null) expected_column_type = actual_column_type;
    }

    if (!boost::iequals(actual_table->column_name(column_id), expected_table->column_name(column_id))) {
      const std::string error_type = "Column name mismatch (column " + std::to_string(column_id) + ")";
      const std::string error_msg = "Actual column name: " + actual_table->column_name(column_id) + "\n" +
                                    "Expected column name: " + expected_table->column_name(column_id);

      print_table_comparison(error_type, error_msg, {{0, column_id}});
      return stream.str();
    }

    if (actual_column_type != expected_column_type) {
      const std::string error_type = "Column type mismatch (column " + std::to_string(column_id) + ")";
      const std::string error_msg =
          "Actual column type: " + data_type_to_string.left.at(actual_table->column_data_type(column_id)) + "\n" +
          "Expected column type: " + data_type_to_string.left.at(expected_table->column_data_type(column_id));

      print_table_comparison(error_type, error_msg, {{1, column_id}});
      return stream.str();
    }

    if (ignore_nullable == IgnoreNullable::No && actual_column_is_nullable != expected_column_is_nullable) {
      const std::string error_type = "Column NULLable mismatch (column " + std::to_string(column_id) + ")";
      const std::string error_msg = std::string{"Actual column is "} + (actual_column_is_nullable ? "" : "NOT ") +
                                    "NULL\n" + std::string{"Expected column is "} +
                                    (expected_column_is_nullable ? "" : "NOT ") + "NULL";

      print_table_comparison(error_type, error_msg, {{2, column_id}});
      return stream.str();
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (actual_table->row_count() != expected_table->row_count()) {
    const std::string error_type = "Row count mismatch";
    const std::string error_msg = "Actual number of rows: " + std::to_string(actual_table->row_count()) + "\n" +
                                  "Expected number of rows: " + std::to_string(expected_table->row_count());

    print_table_comparison(error_type, error_msg);
    return stream.str();
  }

  // sort if order does not matter
  if (order_sensitivity == OrderSensitivity::No) {
    // skip header when sorting
    std::sort(actual_matrix.begin() + HEADER_SIZE, actual_matrix.end());
    std::sort(expected_matrix.begin() + HEADER_SIZE, expected_matrix.end());
  }

  bool has_error = false;
  std::vector<std::pair<uint64_t, uint16_t>> mismatched_cells{};

  const auto highlight_if = [&has_error, &mismatched_cells](bool statement, uint64_t row_id, uint16_t column_id) {
    if (statement) {
      has_error = true;
      mismatched_cells.emplace_back(row_id, column_id);
    }
  };

  // Compare each cell, skipping header
  for (auto row_id = size_t{HEADER_SIZE}; row_id < actual_matrix.size(); row_id++) {
    for (auto column_id = ColumnID{0}; column_id < actual_matrix[row_id].size(); column_id++) {
      if (variant_is_null(actual_matrix[row_id][column_id]) || variant_is_null(expected_matrix[row_id][column_id])) {
        highlight_if(
            !(variant_is_null(actual_matrix[row_id][column_id]) && variant_is_null(expected_matrix[row_id][column_id])),
            row_id, column_id);
      } else if (actual_table->column_data_type(column_id) == DataType::Float) {
        auto left_val = static_cast<double>(boost::get<float>(actual_matrix[row_id][column_id]));
        auto right_val = lossless_variant_cast<double>(expected_matrix[row_id][column_id]);
        Assert(right_val, "Expected double or float in expected_matrix");

        highlight_if(!almost_equals(left_val, *right_val, float_comparison_mode), row_id, column_id);
      } else if (actual_table->column_data_type(column_id) == DataType::Double) {
        auto left_val = boost::get<double>(actual_matrix[row_id][column_id]);
        auto right_val = lossless_variant_cast<double>(expected_matrix[row_id][column_id]);
        Assert(right_val, "Expected double or float in expected_matrix");

        highlight_if(!almost_equals(left_val, *right_val, float_comparison_mode), row_id, column_id);
      } else {
        if (type_cmp_mode == TypeCmpMode::Lenient && (actual_table->column_data_type(column_id) == DataType::Int ||
                                                      actual_table->column_data_type(column_id) == DataType::Long)) {
          auto left_val = lossless_variant_cast<int64_t>(actual_matrix[row_id][column_id]);
          auto right_val = lossless_variant_cast<int64_t>(expected_matrix[row_id][column_id]);
          Assert(left_val && right_val, "Expected int or long in actual_matrix and expected_matrix");

          highlight_if(*left_val != *right_val, row_id, column_id);
        } else {
          highlight_if(actual_matrix[row_id][column_id] != expected_matrix[row_id][column_id], row_id, column_id);
        }
      }
    }
  }

  if (has_error) {
    const std::string error_type = "Cell data mismatch";
    std::string error_msg = "Mismatched cells (row,column): ";
    for (auto cell : mismatched_cells) {
      error_msg += "(" + std::to_string(cell.first - HEADER_SIZE) + "," + std::to_string(cell.second) + ") ";
    }

    print_table_comparison(error_type, error_msg, mismatched_cells);
    return stream.str();
  }

  return std::nullopt;
}

}  // namespace opossum
