#include <unordered_map>
#include <utility>
#include <chrono>
#include <iostream>
#include <bitset>
#include <functional>

#include <boost/functional/hash_fwd.hpp>

#include "all_type_variant.hpp"
#include "functional_dependency_plugin.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "types.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

//constexpr size_t BITSETSIZE = 64;

std::string FunctionalDependencyPlugin::description() const { return "Check for functional dependencies"; }

void FunctionalDependencyPlugin::start() {
  std::cout << "Loading table sf-1" << std::endl;
  const auto& table = BinaryParser::parse("/home/Alexander.Dubrawski/WorkingSets/hyrise/cmake-build-release/tpch_cached_tables/sf-1.000000/customer.bin");
  std::cout << "Table loaded sf-1" << std::endl;
  std::vector<ColumnID> determinant{};
  determinant.push_back(table->column_id_by_name("c_custkey"));
  std::vector<ColumnID> dependent{};
  dependent.push_back(table->column_id_by_name("c_name"));
  dependent.push_back(table->column_id_by_name("c_acctbal"));
  dependent.push_back(table->column_id_by_name("c_phone"));
  dependent.push_back(table->column_id_by_name("c_address"));
  dependent.push_back(table->column_id_by_name("c_comment"));
  auto start = std::chrono::high_resolution_clock::now();
  auto dependency = _check_dependency(table, determinant, dependent);
  auto end = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Sf-1 dependency: " << dependency << std::endl;
  std::cout << "Execution time sf-1: " << ms_int.count() << std::endl;
  std::cout << "Loading table sf-10" << std::endl;
  const auto& table_sf_10 = BinaryParser::parse("/home/Alexander.Dubrawski/WorkingSets/hyrise/cmake-build-release/tpch_cached_tables/sf-10.000000/customer.bin");
  std::cout << "Table loaded sf-10" << std::endl;
  std::vector<ColumnID> determinant_sf_10{};
  determinant_sf_10.push_back(table_sf_10->column_id_by_name("c_custkey"));
  std::vector<ColumnID> dependent_sf_10{};
  dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_name"));
  dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_acctbal"));
  dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_phone"));
  dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_address"));
  dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_comment"));
  start = std::chrono::high_resolution_clock::now();
  dependency = _check_dependency(table_sf_10, determinant_sf_10, dependent_sf_10);
  end = std::chrono::high_resolution_clock::now();
  ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Sf-10 dependency: " << dependency << std::endl;
  std::cout << "Execution time sf-10: " << ms_int.count() << std::endl;
}

void FunctionalDependencyPlugin::stop() {}

bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table,
                                                   const std::vector<ColumnID> determinant,
                                                   const std::vector<ColumnID> dependent) {
  // 1. iterate over determinant
  // 2. Check if determinant as key exists. If not add it.
  // 3. Check if value is the same as determinant. If not break.

  struct Hasher {
    size_t operator()(const std::pair<std::vector<unsigned long long>, std::vector<size_t>> attributes) const {
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

  std::unordered_map<std::pair<std::vector<unsigned long long>, std::vector<size_t>>,
                     std::pair<std::vector<unsigned long long>, std::vector<size_t>>, Hasher>
      dependency;
  for (size_t row_id = 0; row_id < row_count; row_id++) {
    // Inside the first object, we need to save the values of the attributes. If the value is null we save 0.
    // Inside the second object, we need to keep track of which value is null. That way every value combination of a
    // row gets a unique Pair. This is important to be able to hash correctly:
    // {a | null | 0 } -> {Tom} has the pair <[a, 0, 0], [0, 1, 0]>
    // {a | 0 | null } -> {Alex} has the pair <[a, 0, 0], [0, 0, 1]>
    // We are keeping track of the null rows so that we do not generate the same hash for two different attribute
    // values with the value 0 and the value null.
    std::pair<std::vector<unsigned long long>, std::vector<size_t>> row_determinant{std::vector<unsigned long long>(),
                                                                                std::vector<size_t>()};
    auto hash_function = std::hash<pmr_string>{};
    for (const auto column_id : determinant) {
      resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        auto value = table->get_value<ColumnDataType>(column_id, row_id);
        constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
        if (value) {
          if constexpr (!IS_STRING_COLUMN) {
            row_determinant.first.push_back(static_cast<unsigned long long>((value).value()));
          } else {
            row_determinant.first.push_back(static_cast<unsigned long long>(hash_function((value).value())));
          }
          row_determinant.second.push_back(0);
        } else {
          row_determinant.first.push_back(0.0);
          row_determinant.second.push_back(1);
        }
      });
    }
    std::pair<std::vector<unsigned long long>, std::vector<size_t>> row_dependent{std::vector<unsigned long long>(),
                                                                              std::vector<size_t>()};
    for (const auto column_id : dependent) {
      resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        auto value = table->get_value<ColumnDataType>(column_id, row_id);
        constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
        if (value) {
          if constexpr (!IS_STRING_COLUMN) {
            row_dependent.first.push_back(static_cast<unsigned long long>((value).value()));
          } else {
            row_dependent.first.push_back(static_cast<unsigned long long>(hash_function((value).value())));
          }
          row_dependent.second.push_back(0);
        } else {
          row_dependent.first.push_back(0.0);
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