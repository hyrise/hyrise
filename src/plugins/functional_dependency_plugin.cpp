#include <unordered_map>
#include <utility>

#include <boost/functional/hash_fwd.hpp>

#include "all_type_variant.hpp"
#include "functional_dependency_plugin.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

std::string FunctionalDependencyPlugin::description() const { return "Check for functional dependencies"; }

void FunctionalDependencyPlugin::start() {}

void FunctionalDependencyPlugin::stop() {}

bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table,
                                                   const std::vector<ColumnID> determinant,
                                                   const std::vector<ColumnID> dependent) {
  // 1. iterate over determinant
  // 2. Check if determinant as key exists. If not add it.
  // 3. Check if value is the same as determinant. If not break.

  struct Hasher {
    size_t operator()(const std::pair<std::vector<AllTypeVariant>, std::vector<size_t>> attributes) const {
      auto seed = size_t{0};
      for (auto& attribut : attributes.first) {
        boost::hash_combine(seed, attribut);
      }
      for (auto& attribut : attributes.second) {
        boost::hash_combine(seed, attribut);
      }
      return seed;
    }
  };

  const auto row_count = table->row_count();

  std::unordered_map<std::pair<std::vector<AllTypeVariant>, std::vector<size_t>>,
                     std::pair<std::vector<AllTypeVariant>, std::vector<size_t>>, Hasher>
      dependency;
  for (size_t row_id = 0; row_id < row_count; row_id++) {
    // Inside the first object, we need to save the values of the attributes. If the value is null we save 0.
    // Inside the second object, we need to keep track of which value is null. That way every value combination of a
    // row gets a unique Pair. This is important to be able to hash correctly:
    // {a | null | 0 } -> {Tom} has the pair <[a, 0, 0], [0, 1, 0]>
    // {a | 0 | null } -> {Alex} has the pair <[a, 0, 0], [0, 0, 1]>
    // We are keeping track of the null rows so that we do not generate the same hash for two different attribute
    // values with the value 0 and the value null.
    std::pair<std::vector<AllTypeVariant>, std::vector<size_t>> row_determinant{std::vector<AllTypeVariant>(),
                                                                                std::vector<size_t>()};
    for (const auto column_id : determinant) {
      resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        auto value = table->get_value<ColumnDataType>(column_id, row_id);
        if (value) {
          row_determinant.first.push_back((value).value());
          row_determinant.second.push_back(0);
        } else {
          row_determinant.first.push_back(0);
          row_determinant.second.push_back(1);
        }
      });
    }
    std::pair<std::vector<AllTypeVariant>, std::vector<size_t>> row_dependent{std::vector<AllTypeVariant>(),
                                                                              std::vector<size_t>()};
    for (const auto column_id : dependent) {
      resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        auto value = table->get_value<ColumnDataType>(column_id, row_id);
        if (value) {
          row_dependent.first.push_back((value).value());
          row_dependent.second.push_back(0);
        } else {
          row_dependent.first.push_back(0);
          row_dependent.second.push_back(1);
        }
      });
    }
    if (dependency.contains(row_determinant)) {
      if (dependency[row_determinant] != row_dependent) {
        return false;
      }
    } else {
      dependency[row_determinant] = row_dependent;
    }
  }
  return true;
}

EXPORT_PLUGIN(FunctionalDependencyPlugin)

}  // namespace opossum