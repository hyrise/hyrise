#include "ucc_validation_rule_set.hpp"

#include "hyrise.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/sort.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/timer.hpp"

namespace opossum {

UCCValidationRuleSet::UCCValidationRuleSet() : AbstractDependencyValidationRule(DependencyType::Unique) {}

std::shared_ptr<ValidationResult> UCCValidationRuleSet::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.determinants.size() == 1, "Invalid determinants for UCC set validation");

  // Timer timer;
  const auto determinant = candidate.determinants[0];
  const auto table_name = determinant.table_name;
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  const auto column_id = determinant.column_id;
  auto column_type = table->column_data_type(column_id);

  bool is_valid = true;
  resolve_data_type(column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    const auto collect_dictionaries = [&]() {
      Assert(table && table->type() == TableType::Data, "Expected Data table");
      std::vector<std::shared_ptr<const pmr_vector<ColumnDataType>>> dictionaries;
      dictionaries.reserve(table->chunk_count());
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        if (!chunk) continue;
        const auto& segment = chunk->get_segment(column_id);
        if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
          dictionaries.emplace_back(dictionary_segment->dictionary());
          if (dictionary_segment->dictionary()->size() < segment->size()) {
            is_valid = false;
            return dictionaries;
          }
        } else {
          Fail("Could not resolve dictionary segment");
        }
      }
      return dictionaries;
    };

    const auto& dictionaries = collect_dictionaries();
    if (!is_valid) return;
    std::unordered_set<ColumnDataType> values;
    size_t max_dict_size{0};
    size_t dict_size_sum{0};
    for (const auto& dictionary : dictionaries) {
      max_dict_size = std::max(max_dict_size, dictionary->size());
    }
    values.reserve(max_dict_size);
    for (const auto& dictionary : dictionaries) {
      values.insert(dictionary->cbegin(), dictionary->cend());
      dict_size_sum += dictionary->size();
      if (values.size() < dict_size_sum) {
        is_valid = false;
        return;
      }
    }
  });

  if (!is_valid) return INVALID_VALIDATION_RESULT;
  const auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Valid);
  result->constraints[table_name].emplace_back(
      std::make_shared<TableKeyConstraint>(std::unordered_set<ColumnID>{column_id}, KeyConstraintType::UNIQUE));
  return result;
}

}  // namespace opossum
