#include "column_comparison_table_scan_impl.hpp"

#include <memory>
#include <string>
#include <type_traits>

#include "storage/chunk.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

ColumnComparisonTableScanImpl::ColumnComparisonTableScanImpl(std::shared_ptr<const Table> in_table,
                                                             const ColumnID left_column_id,
                                                             const PredicateCondition& predicate_condition,
                                                             const ColumnID right_column_id)
    : BaseTableScanImpl{in_table, left_column_id, predicate_condition}, _right_column_id{right_column_id} {}

PosList ColumnComparisonTableScanImpl::scan_chunk(ChunkID chunk_id) {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto left_column_type = _in_table->column_type(_left_column_id);
  const auto right_column_type = _in_table->column_type(_right_column_id);

  const auto left_column = chunk->get_column(_left_column_id);
  const auto right_column = chunk->get_column(_right_column_id);

  auto matches_out = PosList{};

  resolve_data_and_column_type(left_column_type, *left_column, [&](auto left_type, auto& typed_left_column) {
    resolve_data_and_column_type(right_column_type, *right_column, [&](auto right_type, auto& typed_right_column) {
      using LeftColumnType = typename std::decay<decltype(typed_left_column)>::type;
      using RightColumnType = typename std::decay<decltype(typed_right_column)>::type;

      using LeftType = typename decltype(left_type)::type;
      using RightType = typename decltype(right_type)::type;

      /**
       * This generic lambda is instantiated for each type (int, long, etc.) and
       * each column type (value, dictionary, reference column) per column!
       * That’s 3x5 combinations each and 15x15=225 in total. However, not all combinations are valid or possible.
       * Only data columns (value, dictionary) or reference columns will be compared, as a table with both data and
       * reference columns is ruled out. Moreover it is not possible to compare strings to any of the four numerical
       * data types. Therefore, we need to check for these cases and exclude them via the constexpr-if which
       * reduces the number of combinations to 85.
       */

      constexpr auto left_is_reference_column = (std::is_same<LeftColumnType, ReferenceColumn>{});
      constexpr auto right_is_reference_column = (std::is_same<RightColumnType, ReferenceColumn>{});

      constexpr auto neither_is_reference_column = !left_is_reference_column && !right_is_reference_column;
      constexpr auto both_are_reference_columns = left_is_reference_column && right_is_reference_column;

      constexpr auto left_is_string_column = (std::is_same<LeftType, std::string>{});
      constexpr auto right_is_string_column = (std::is_same<RightType, std::string>{});

      constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
      constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

      // clang-format off
      if constexpr((neither_is_reference_column || both_are_reference_columns) &&
                   (neither_is_string_column || both_are_string_columns)) {
        auto left_column_iterable = create_iterable_from_column<LeftType>(typed_left_column);
        auto right_column_iterable = create_iterable_from_column<RightType>(typed_right_column);

        left_column_iterable.require_single_functor_call();
        right_column_iterable.require_single_functor_call();

        left_column_iterable.with_iterators([&](auto left_it, auto left_end) {
          right_column_iterable.with_iterators([&](auto right_it, auto right_end) {
            with_comparator(_predicate_condition, [&](auto comparator) {
              this->_binary_scan(comparator, left_it, left_end, right_it, chunk_id, matches_out);
            });
          });
        });
      } else {
        Fail("Invalid column combination detected!");   // NOLINT - cpplint.py does not know about constexpr
      }
      // clang-format on
    });
  });

  return matches_out;
}

}  // namespace opossum
