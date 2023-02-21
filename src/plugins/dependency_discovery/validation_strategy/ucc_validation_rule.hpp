#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace hyrise {

class Table;

class UccValidationRule : public AbstractDependencyValidationRule {
 public:
  UccValidationRule();

 protected:
  ValidationResult _on_validate(const AbstractDependencyCandidate& candidate) const override;

 private:
  /**
   * Checks whether individual DictionarySegments contain duplicates. This is an efficient operation as the check is
   * simply comparing the length of the dictionary to that of the attribute vector. This function can therefore be used
   * for an early-out before the more expensive cross-segment uniqueness check.
   */
  template <typename ColumnDataType>
  static bool _dictionary_segments_contain_duplicates(const std::shared_ptr<Table>& table, ColumnID column_id);

  /**
   * Checks whether the given table contains only unique values by inserting all values into an unordered set. If for
   * any table segment the size of the set increases by less than the number of values in that segment, we know that
   * there must be a duplicate and return false. Otherwise, returns true.
   */
  template <typename ColumnDataType>
  static bool _uniqueness_holds_across_segments(const std::shared_ptr<Table>& table, ColumnID column_id);
};

}  // namespace hyrise
