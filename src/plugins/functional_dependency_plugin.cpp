#include <unordered_map>

#include <boost/functional/hash_fwd.hpp>

#include "functional_dependency_plugin.hpp"
#include "storage/table.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

std::string FunctionalDependencyPlugin::description() const { return "Check for functional dependencies"; }

void FunctionalDependencyPlugin::start() {
}

void FunctionalDependencyPlugin::stop() {
}

bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table, const std::vector<ColumnID> determinant, const std::vector<ColumnID> dependent) {
  // 1. iterate over determinant
  // 2. Check if determinant as key exists. If not add it.
  // 3. Check if value is the same as determinant. If not break.

  struct Hasher {
    size_t operator()(const std::vector<AllTypeVariant> attributes) const {
      auto seed = size_t{0};
      for (auto& attribut : attributes) {
        boost::hash_combine(seed, attribut);
      }
      return seed;
    }
  };

  auto row_cout = table->row_count();
  std::unordered_map<std::vector<AllTypeVariant>, std::vector<AllTypeVariant>, Hasher> dependency;
  for (size_t row_id = 0; row_id < row_cout; row_id++) {
    std::vector<AllTypeVariant> row_determinant;
    for (auto column_id : determinant) {
      resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
                 using ColumnDataType = typename
                     decltype(data_type_t)::type;
                  row_determinant.push_back((table->get_value<ColumnDataType>(column_id, row_id)).value());
      });
    }
    std::vector<AllTypeVariant> row_dependent;
    for (auto column_id : dependent) {
      resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
                 using ColumnDataType = typename
                     decltype(data_type_t)::type;
                  row_dependent.push_back((table->get_value<ColumnDataType>(column_id, row_id)).value());
      });
    }
    if (dependency.contains(row_determinant)) {
      if(dependency[row_determinant] != row_dependent) {
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