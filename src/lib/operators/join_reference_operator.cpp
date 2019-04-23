#include "join_reference_operator.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace {

template <typename T>
std::vector<T> concatenate(const std::vector<T>& l, const std::vector<T>& r) {
  auto result = l;
  result.insert(result.end(), r.begin(), r.end());
  return result;
}

}  // namespace

namespace opossum {

JoinReferenceOperator::JoinReferenceOperator(const std::shared_ptr<const AbstractOperator>& left,
                                             const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                             const OperatorJoinPredicate& primary_predicate,
                                             const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::JoinReference, left, right, mode, primary_predicate, secondary_predicates) {}

const std::string JoinReferenceOperator::name() const { return "JoinReference"; }

std::shared_ptr<const Table> JoinReferenceOperator::_on_execute() {
  const auto output_table = _initialize_output_table(TableType::Data);
  const auto left_table = input_table_left();
  const auto right_table = input_table_right();

  const auto left_rows = input_table_left()->get_rows();
  const auto right_rows = input_table_right()->get_rows();

  auto null_row_left = std::vector<AllTypeVariant>(left_table->column_count(), NullValue{});
  auto null_row_right = std::vector<AllTypeVariant>(right_table->column_count(), NullValue{});

  switch (_mode) {
    case JoinMode::Inner:
      for (const auto& left_row : left_rows) {
        for (const auto& right_row : right_rows) {
          if (_rows_match(left_row, right_row)) {
            output_table->append(concatenate(left_row, right_row));
          }
        }
      }
      break;

    case JoinMode::Left:
      for (const auto& left_row : left_rows) {
        auto has_match = false;

        for (const auto& right_row : right_rows) {
          if (_rows_match(left_row, right_row)) {
            has_match = true;
            output_table->append(concatenate(left_row, right_row));
          }
        }

        if (!has_match) {
          output_table->append(concatenate(left_row, null_row_right));
        }
      }
      break;

    case JoinMode::Right:
      for (const auto& right_row : right_rows) {
        auto has_match = false;

        for (const auto& left_row : left_rows) {
          if (_rows_match(left_row, right_row)) {
            has_match = true;
            output_table->append(concatenate(left_row, right_row));
          }
        }

        if (!has_match) {
          output_table->append(concatenate(null_row_left, right_row));
        }
      }
      break;

    case JoinMode::FullOuter: {
      auto left_matches = std::vector<bool>(left_table->row_count(), false);
      auto right_matches = std::vector<bool>(right_table->row_count(), false);

      for (size_t left_row_idx{0}; left_row_idx < left_rows.size(); ++left_row_idx) {
        const auto& left_row = left_rows[left_row_idx];

        for (size_t right_row_idx{0}; right_row_idx < right_rows.size(); ++right_row_idx) {
          const auto& right_row = right_rows[right_row_idx];

          if (_rows_match(left_row, right_row)) {
            output_table->append(concatenate(left_row, right_row));
            left_matches[left_row_idx] = true;
            right_matches[right_row_idx] = true;
          }
        }
      }

      for (size_t left_row_idx{0}; left_row_idx < left_table->row_count(); ++left_row_idx) {
        if (!left_matches[left_row_idx]) {
          output_table->append(concatenate(left_rows[left_row_idx], null_row_right));
        }
      }
      for (size_t right_row_idx{0}; right_row_idx < right_table->row_count(); ++right_row_idx) {
        if (!right_matches[right_row_idx]) {
          output_table->append(concatenate(null_row_left, right_rows[right_row_idx]));
        }
      }
    } break;

    case JoinMode::Semi: {
      for (const auto& left_row : left_rows) {
        auto has_match = false;

        for (const auto& right_row : right_rows) {
          if (_rows_match(left_row, right_row)) {
            has_match = true;
            break;
          }
        }

        if (has_match) {
          output_table->append(left_row);
        }
      }
    } break;

    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse: {
      for (const auto& left_row : left_rows) {
        auto has_no_match = true;

        for (const auto& right_row : right_rows) {
          if (_rows_match(left_row, right_row)) {
            has_no_match = false;
            break;
          }
        }

        if (has_no_match) {
          output_table->append(left_row);
        }
      }
    } break;

    case JoinMode::Cross:
      Fail("Cross join not supported");
      break;
  }

  return output_table;
}

bool JoinReferenceOperator::_rows_match(const std::vector<AllTypeVariant>& row_left,
                                        const std::vector<AllTypeVariant>& row_right) const {
  if (!_predicate_matches(_primary_predicate, row_left, row_right)) {
    return false;
  }

  for (const auto& secondary_predicate : _secondary_predicates) {
    if (!_predicate_matches(secondary_predicate, row_left, row_right)) {
      return false;
    }
  }

  return true;
}

bool JoinReferenceOperator::_predicate_matches(const OperatorJoinPredicate& predicate,
                                               const std::vector<AllTypeVariant>& row_left,
                                               const std::vector<AllTypeVariant>& row_right) const {
  auto result = false;
  const auto variant_left = row_left.at(predicate.column_ids.first);
  const auto variant_right = row_right.at(predicate.column_ids.second);

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

std::shared_ptr<AbstractOperator> JoinReferenceOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinReferenceOperator>(copied_input_left, copied_input_right, _mode, _primary_predicate,
                                                 _secondary_predicates);
}

void JoinReferenceOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
