#include "join_verification.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "null_value.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/operator_join_predicate.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

template <typename T>
std::vector<T> concatenate(const std::vector<T>& left, const std::vector<T>& right) {
  auto result = left;
  result.insert(result.end(), right.begin(), right.end());
  return result;
}

}  // namespace

namespace hyrise {

bool JoinVerification::supports(const JoinConfiguration /*config*/) {
  return true;
}

JoinVerification::JoinVerification(const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                   const OperatorJoinPredicate& primary_predicate,
                                   const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::JoinVerification, left, right, mode, primary_predicate, secondary_predicates) {
}

const std::string& JoinVerification::name() const {
  static const auto name = std::string{"JoinVerification"};
  return name;
}

std::shared_ptr<const Table> JoinVerification::_on_execute() {
  const auto output_table = _build_output_table({}, TableType::Data);

  const auto left_tuples = left_input_table()->get_rows();
  const auto right_tuples = right_input_table()->get_rows();

  // Tuples with NULLs used to fill up tuples in outer joins that do not find a match
  const auto null_tuple_left = Tuple(left_input_table()->column_count(), NullValue{});
  const auto null_tuple_right = Tuple(right_input_table()->column_count(), NullValue{});

  switch (_mode) {
    case JoinMode::Inner:
      for (const auto& left_tuple : left_tuples) {
        for (const auto& right_tuple : right_tuples) {
          if (_tuples_match(left_tuple, right_tuple)) {
            output_table->append(concatenate(left_tuple, right_tuple));
          }
        }
      }
      break;

    case JoinMode::Left:
      for (const auto& left_tuple : left_tuples) {
        auto has_match = false;

        for (const auto& right_tuple : right_tuples) {
          if (_tuples_match(left_tuple, right_tuple)) {
            has_match = true;
            output_table->append(concatenate(left_tuple, right_tuple));
          }
        }

        if (!has_match) {
          output_table->append(concatenate(left_tuple, null_tuple_right));
        }
      }
      break;

    case JoinMode::Right:
      for (const auto& right_tuple : right_tuples) {
        auto has_match = false;

        for (const auto& left_tuple : left_tuples) {
          if (_tuples_match(left_tuple, right_tuple)) {
            has_match = true;
            output_table->append(concatenate(left_tuple, right_tuple));
          }
        }

        if (!has_match) {
          output_table->append(concatenate(null_tuple_left, right_tuple));
        }
      }
      break;

    case JoinMode::FullOuter: {
      // Track which tuples from each side have matches
      const auto left_row_count = left_input_table()->row_count();
      const auto right_row_count = right_input_table()->row_count();
      auto left_matches = std::vector<bool>(left_row_count, false);
      auto right_matches = std::vector<bool>(right_row_count, false);

      const auto left_tuple_count = left_tuples.size();
      const auto right_tuple_count = right_tuples.size();
      for (auto left_tuple_idx = size_t{0}; left_tuple_idx < left_tuple_count; ++left_tuple_idx) {
        const auto& left_tuple = left_tuples[left_tuple_idx];

        for (auto right_tuple_idx = size_t{0}; right_tuple_idx < right_tuple_count; ++right_tuple_idx) {
          const auto& right_tuple = right_tuples[right_tuple_idx];

          if (_tuples_match(left_tuple, right_tuple)) {
            output_table->append(concatenate(left_tuple, right_tuple));
            left_matches[left_tuple_idx] = true;
            right_matches[right_tuple_idx] = true;
          }
        }
      }

      // Add tuples without matches to output table.
      for (auto left_tuple_idx = size_t{0}; left_tuple_idx < left_row_count; ++left_tuple_idx) {
        if (!left_matches[left_tuple_idx]) {
          output_table->append(concatenate(left_tuples[left_tuple_idx], null_tuple_right));
        }
      }

      for (auto right_tuple_idx = size_t{0}; right_tuple_idx < right_row_count; ++right_tuple_idx) {
        if (!right_matches[right_tuple_idx]) {
          output_table->append(concatenate(null_tuple_left, right_tuples[right_tuple_idx]));
        }
      }
    } break;

    case JoinMode::Semi: {
      for (const auto& left_tuple : left_tuples) {
        const auto has_match = std::any_of(right_tuples.begin(), right_tuples.end(), [&](const auto& right_tuple) {
          return _tuples_match(left_tuple, right_tuple);
        });

        if (has_match) {
          output_table->append(left_tuple);
        }
      }
    } break;

    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse: {
      for (const auto& left_tuple : left_tuples) {
        const auto has_no_match = std::none_of(right_tuples.begin(), right_tuples.end(), [&](const auto& right_tuple) {
          return _tuples_match(left_tuple, right_tuple);
        });

        if (has_no_match) {
          output_table->append(left_tuple);
        }
      }
    } break;

    case JoinMode::Cross:
      Fail("Cross join not supported");
      break;
  }

  return output_table;
}

bool JoinVerification::_tuples_match(const Tuple& tuple_left, const Tuple& tuple_right) const {
  if (!_evaluate_predicate(_primary_predicate, tuple_left, tuple_right)) {
    return false;
  }

  return std::all_of(_secondary_predicates.begin(), _secondary_predicates.end(), [&](const auto& secondary_predicate) {
    return _evaluate_predicate(secondary_predicate, tuple_left, tuple_right);
  });
}

bool JoinVerification::_evaluate_predicate(const OperatorJoinPredicate& predicate, const Tuple& tuple_left,
                                           const Tuple& tuple_right) const {
  auto result = false;
  const auto& variant_left = tuple_left[predicate.column_ids.first];
  const auto& variant_right = tuple_right[predicate.column_ids.second];

  if (variant_is_null(variant_left) || variant_is_null(variant_right)) {
    // AntiNullAsTrue is the only JoinMode that treats null-booleans as TRUE, all others treat it as FALSE
    return _mode == JoinMode::AntiNullAsTrue;
  }

  resolve_data_type(data_type_from_all_type_variant(variant_left), [&](const auto data_type_left_t) {
    using ColumnDataTypeLeft = typename decltype(data_type_left_t)::type;

    resolve_data_type(data_type_from_all_type_variant(variant_right), [&](const auto data_type_right_t) {
      using ColumnDataTypeRight = typename decltype(data_type_right_t)::type;

      if constexpr (std::is_same_v<ColumnDataTypeLeft, pmr_string> == std::is_same_v<ColumnDataTypeRight, pmr_string>) {
        with_comparator(predicate.predicate_condition, [&](const auto comparator) {
          result =
              comparator(boost::get<ColumnDataTypeLeft>(variant_left), boost::get<ColumnDataTypeRight>(variant_right));
        });
      } else {
        Fail("Cannot compare string with non-string type");
      }
    });
  });

  return result;
}

std::shared_ptr<AbstractOperator> JoinVerification::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<JoinVerification>(copied_left_input, copied_right_input, _mode, _primary_predicate,
                                            _secondary_predicates);
}

void JoinVerification::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
