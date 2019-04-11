#include "join_reference_operator.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace {

template<typename T>
std::vector<T> concatenate(const std::vector<T>& l, const std::vector<T>& r) {
  auto result = l;
  result.insert(result.end(), r.begin(), r.end());
  result.insert(result.end(), r.begin(), r.end());
  return result;
}

}  // namespace

namespace opossum {

JoinReferenceOperator::JoinReferenceOperator(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const OperatorJoinPredicate& primary_predicate, const std::vector<OperatorJoinPredicate>& secondary_predicates):
                     AbstractJoinOperator(OperatorType::JoinReference, left, right, mode, primary_predicate, secondary_predicates) {

}

const std::string JoinReferenceOperator::name() const { return "JoinReference"; }

std::shared_ptr<const Table> JoinReferenceOperator::_on_execute() {
  const auto output_table = _initialize_output_table();
  const auto left_table = input_table_left();
  const auto right_table = input_table_right();

  switch (_mode) {
    case JoinMode::Inner:
      for (size_t left_row_idx{0}; left_row_idx < left_table->row_count(); ++left_row_idx) {
        for (size_t right_row_idx{0}; right_row_idx < right_table->row_count(); ++right_row_idx) {
          if (_rows_match(left_row_idx, right_row_idx)) {
            output_table->append(concatenate(left_table->get_row(left_row_idx), right_table->get_row(left_row_idx)));
          }
        }
      }
      break;

    case JoinMode::Left:
      for (size_t left_row_idx{0}; left_row_idx < left_table->row_count(); ++left_row_idx) {
        auto has_match = false;
        for (size_t right_row_idx{0}; right_row_idx < right_table->row_count(); ++right_row_idx) {
          if (_rows_match(left_row_idx, right_row_idx)) {
            has_match = true;
            output_table->append(concatenate(left_table->get_row(left_row_idx), right_table->get_row(left_row_idx)));
          }
        }

        if (!has_match) {
          output_table->append(concatenate(left_table->get_row(left_row_idx), std::vector<AllTypeVariant>));
        }
      }
      break;

    case JoinMode::Right:
      break;

    case JoinMode::FullOuter:
      break;

    case JoinMode::Cross:
      break;

    case JoinMode::Semi:
      break;

    case JoinMode::AntiNullAsTrue:
      break;

    case JoinMode::AntiNullAsFalse:
      break;

  }

  return output_table;
}

bool JoinReferenceOperator::_rows_match(size_t left_row_idx, size_t right_row_idx) const {
  const auto row_left = input_table_left()->get_row(left_row_idx);
  const auto row_right = input_table_left()->get_row(right_row_idx);

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

  resolve_data_type(data_type_from_all_type_variant(variant_left), [&](const auto data_type_left_t) {
    const auto ColumnDataTypeLeft = typename decltype(data_type_left_t)::type;
    resolve_data_type(data_type_from_all_type_variant(variant_right), [&](const auto data_type_right_t) {
      const auto ColumnDataTypeRight = typename decltype(data_type_left_t)::type;

      if constexpr (std::is_same_v<ColumnDataTypeLeft, pmr_string> == std::is_same_v<ColumnDataTypeRight, pmr_string>) {
        with_comparator(predicate.predicate_condition, [&](const auto comparator) {
          result = comparator(boost::get<ColumnDataTypeLeft>(variant_left), boost::get<ColumnDataTypeLeft>(variant_right));
        });
      } else {
        Fail("Cannot compare string with non-string type");
      }
    });
  });

  return result;
}

}  // namespace opossum
