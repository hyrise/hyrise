#include "ucc_validation_rule_spider.hpp"

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

UCCValidationRuleSpider::UCCValidationRuleSpider() : AbstractDependencyValidationRule(DependencyType::Unique) {}

std::shared_ptr<ValidationResult> UCCValidationRuleSpider::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.determinants.size() == 1, "Invalid determinants for UCC set validation");

  // Timer timer;
  const auto determinant = candidate.determinants[0];
  const auto table_name = determinant.table_name;
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  const auto column_id = determinant.column_id;
  Assert(column_id < table->column_count(), "ivalid column id");
  auto column_type = table->column_data_type(column_id);

  bool is_valid = true;
  resolve_data_type(column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    using Dictionaries = std::vector<std::shared_ptr<const pmr_vector<ColumnDataType>>>;
    Dictionaries dictionaries;

    const auto gather_dictionaries = [&]() {
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        if (!chunk) continue;
        const auto& segment = chunk->get_segment(column_id);
        if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
          const auto dictionary = dictionary_segment->dictionary();
          if (!dictionary->empty()) {
            dictionaries.emplace_back(dictionary);
          }
        } else {
          Fail("Could not resolve dictionary segment");
        }
      }
    };

    gather_dictionaries();

    std::vector<ChunkOffset> positions(dictionaries.size(), ChunkOffset{0});
    Assert(dictionaries.size() == positions.size(), "invalid determinant positions");
    auto finished_dictionaries = ChunkID{0};
    const auto fetch_next_value = [&]() {
      auto current_smallest_value = ColumnDataType{0};
      auto current_smallest_dictionary = ChunkID{0};
      bool init = false;
      for (auto dictionary_id = ChunkID{0}; dictionary_id < positions.size(); ++dictionary_id) {
        const auto& value_pointer = positions[dictionary_id];
        const auto& dictionary = dictionaries.at(dictionary_id);
        if (value_pointer == dictionary->size()) continue;
        const auto value = dictionary->at(value_pointer);
        if (!init || value < current_smallest_value) {
          init = true;
          current_smallest_value = dictionary->at(value_pointer);
          current_smallest_dictionary = dictionary_id;
          continue;
        } else if (value == current_smallest_value) {
          return false;
        }
      }
      for (auto dictionary_id = ChunkID{0}; dictionary_id < positions.size(); ++dictionary_id) {
        const auto& value_pointer = positions[dictionary_id];
        const auto& dictionary = dictionaries.at(dictionary_id);
        if (value_pointer == dictionary->size()) continue;
        auto n_v = uint8_t{0};
        if (dictionary->at(value_pointer) == current_smallest_value) {
          if (n_v > 0) {
            return false;
          }
          if (value_pointer + 1 == dictionary->size()) ++finished_dictionaries;
          ++positions[dictionary_id];
          ++n_v;
        }
      }
      return true;
    };

    while (finished_dictionaries < dictionaries.size()) {
      if (!fetch_next_value()) {
        is_valid = false;
        break;
      }
    }
  });

  if (!is_valid) {
    return INVALID_VALIDATION_RESULT;
  }
  auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Valid);
  result->constraints[table_name].emplace_back(
      std::make_shared<TableKeyConstraint>(std::unordered_set<ColumnID>{column_id}, KeyConstraintType::UNIQUE));
  return result;
}

}  // namespace opossum
