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

struct Hasher {
  size_t operator()(std::vector<long> attributes) const {
    auto seed = size_t{0};
    for (auto& attribut : attributes) {
      boost::hash_combine(seed, attribut);
    }
    // for (auto& attribut : attributes.second) {
    //   boost::hash_combine(seed, attribut);
    // }
    return seed;
  }
};

//constexpr size_t BITSETSIZE = 64;

std::string FunctionalDependencyPlugin::description() const { return "Check for functional dependencies"; }

void FunctionalDependencyPlugin::start() {
  std::cout << "Loading table sf-1" << std::endl;
  const auto& table = BinaryParser::parse("hyrise/cmake-build-release/tpch_cached_tables/sf-1.000000/customer.bin");
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
  std::cout << "Execution time sf-1: " << ms_int.count() << "ms" << std::endl;
  std::cout << "Loading table sf-10" << std::endl;
  const auto& table_sf_10 = BinaryParser::parse("hyrise/cmake-build-release/tpch_cached_tables/sf-10.000000/customer.bin");
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
  std::cout << "Execution time sf-10: " << ms_int.count() << "ms" << std::endl;
}

void FunctionalDependencyPlugin::stop() {}

void FunctionalDependencyPlugin::_process_column_data_string(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<long> &process_column){
  const auto row_count = table->row_count();
  std::unordered_map<pmr_string, long, std::hash<pmr_string>> dictionary;
  for (size_t row_id = 0; row_id < row_count; row_id++) {
    auto value = table->get_value<pmr_string>(column_id, row_id);
    if (value) {
      auto row_value = value.value();
      if (dictionary.contains(row_value)) {
        process_column.push_back(dictionary[row_value]);
      } else {
        auto value_id = dictionary.size() + 1;
        dictionary[row_value] = value_id;
        process_column.push_back(value_id);
      }
    } else {
      process_column.push_back(0);
    }
  }
}

void FunctionalDependencyPlugin::_process_column_data_numeric(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<long> &process_column){
  const auto row_count = table->row_count();
  resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
    for (size_t row_id = 0; row_id < row_count; row_id++) {
      auto value = table->get_value<ColumnDataType>(column_id, row_id);
      if constexpr (!IS_STRING_COLUMN) {
        // TODO: Build in assertion, we only accept numeric values inside this function.
        auto row_value = static_cast<long>(value.value());
        process_column.push_back(row_value);
      }
    }
  });
}

void FunctionalDependencyPlugin::_process_column_data_numeric_null(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<long> &process_column,  std::vector<long> &null_column){
  const auto row_count = table->row_count();
  resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
    for (size_t row_id = 0; row_id < row_count; row_id++) {
      auto value = table->get_value<ColumnDataType>(column_id, row_id);
      if (value) {
        if constexpr (!IS_STRING_COLUMN) {
          // TODO: Build in assertion, we only accept numeric values inside this function.
          auto row_value = static_cast<long>(value.value());
          process_column.push_back(row_value);
        }
      } else {
        null_column[row_id] = 1;
        process_column.push_back(0);
      }
    }
  });
}


bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table,
                                                   const std::vector<ColumnID> determinant,
                                                   const std::vector<ColumnID> dependent) {
  
  std::unordered_map<std::vector<long>, std::vector<long>, Hasher> dependency;
  
  const auto determinant_count = determinant.size();
  const auto total_attribute_count = determinant.size() + dependent.size();
  const auto row_count = table->row_count();

  std::vector<std::vector<long>> normalized_values;
  std::vector<std::vector<long>> null_values;
  std::vector<ColumnID> column_ids;

  column_ids.reserve(determinant.size() + dependent.size());
  column_ids.insert(column_ids.end(), determinant.begin(), determinant.end());
  column_ids.insert(column_ids.end(), dependent.begin(), dependent.end());

  auto start = std::chrono::high_resolution_clock::now();

  std::vector<bool> column_nullable;
  for (const auto column_id : column_ids) {
     resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
      if constexpr (!IS_STRING_COLUMN) {
         column_nullable.push_back(table->column_is_nullable(column_id));
      } else {
        column_nullable.push_back(false);
      }
     });
  }

  for (auto column_idx = size_t{0}; column_idx < column_ids.size(); column_idx++) {
    resolve_data_type(table->column_data_type(column_ids[column_idx]), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
      std::vector<long> column_normalized_values;
      column_normalized_values.reserve(row_count);
      if constexpr (IS_STRING_COLUMN) {
        FunctionalDependencyPlugin::_process_column_data_string(table, column_ids[column_idx], column_normalized_values);
        normalized_values.push_back(std::move(column_normalized_values));
      }
      else if (!column_nullable[column_idx]) {
        FunctionalDependencyPlugin::_process_column_data_numeric(table, column_ids[column_idx], column_normalized_values);
        normalized_values.push_back(std::move(column_normalized_values));
      } else {
        std::vector<long> column_null_values(row_count, 0);
        FunctionalDependencyPlugin::_process_column_data_numeric_null(table, column_ids[column_idx], column_normalized_values, column_null_values);
        normalized_values.push_back(std::move(column_normalized_values));
        null_values.push_back(std::move(column_null_values));
      }
    });
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "-> Process data execution time: " << ms_int.count() << "ms" << std::endl;
  start = std::chrono::high_resolution_clock::now();
  for (size_t row_id = 0; row_id < row_count; row_id++) {
    std::vector<long> row_determinant;
    std::vector<long> row_dependent;
    for (size_t i = 0; i < determinant_count; i++) {
      row_determinant.push_back(normalized_values[i][row_id]);
    }
    size_t null_index = 0;
    for (size_t i = 0; i < determinant_count; i++) {
      if (column_nullable[i]) {
        row_determinant.push_back(null_values[null_index][row_id]);
        null_index++;
      }
    }
    for (size_t i = determinant_count; i < total_attribute_count ; i++) {
      row_dependent.push_back(normalized_values[i][row_id]);
    }
    for (size_t i = determinant_count; i < total_attribute_count ; i++) {
      if (column_nullable[i]) {
        row_dependent.push_back(null_values[null_index][row_id]);
        null_index++;
      }
    }
    if (dependency.contains(row_determinant)) {
      if (dependency[row_determinant] != row_dependent) {
        end = std::chrono::high_resolution_clock::now();
        ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "-> Find FD data execution time: " << ms_int.count() << "ms" << std::endl;
        return false;
      }
    } else {
      dependency[row_determinant] = row_dependent;
    }
  }
  end = std::chrono::high_resolution_clock::now();
  ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "-> Find FD data execution time: " << ms_int.count() << "ms" << std::endl;
  return true;
}


// bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table,
//                                                    const std::vector<ColumnID> determinant,
//                                                    const std::vector<ColumnID> dependent) {

//   std::unordered_map<std::pair<std::vector<long long>, std::vector<size_t>>,
//                      std::pair<std::vector<long long>, std::vector<size_t>>, Hasher>
//       dependency;

//   auto hash_function = std::hash<pmr_string>{};
//   const auto row_count = table->row_count();
//   std::vector<std::vector<unsigned long long>> normalized_values;
//   std::vector<std::vector<unsigned long long>> null_values;
//   normalized_values.reserve(determinant.size() + dependent.size());
//   null_values.reserve(determinant.size() + dependent.size());



//   for (const auto column_id : determinant) {
//     std::vector<long long> column_normalized_values;
//     std::vector<long long> column_null_values;
//     column_normalized_values.reserve(row_count);
//     column_null_values.reserve(row_count);
//     resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
//       using ColumnDataType = typename decltype(data_type_t)::type;
//       constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
//       for (size_t row_id = 0; row_id < row_count; row_id++) {
//         auto value = table->get_value<ColumnDataType>(column_id, row_id);
//         if (value) {
//           if constexpr (!IS_STRING_COLUMN) {
//             column_normalized_values.push_back(static_cast<long long>((value).value()));
//           } else {
//             column_normalized_values.push_back(static_cast<long long>(hash_function((value).value())));
//           }
//           column_null_values.push_back(0);
//         } else {
//           column_normalized_values.push_back(0.0);
//           column_null_values.push_back(1);
//         }
//       }
//      });
//     normalized_values.push_back(std::move(column_normalized_values));
//     null_values.push_back(std::move(column_null_values));
//   }
//   for (const auto column_id : dependent) {
//     std::vector<long long> column_normalized_values;
//     std::vector<long long> column_null_values;
//     column_normalized_values.reserve(row_count);
//     column_null_values.reserve(row_count);
//     resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
//       using ColumnDataType = typename decltype(data_type_t)::type;
//       constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
//       for (size_t row_id = 0; row_id < row_count; row_id++) {
//         auto value = table->get_value<ColumnDataType>(column_id, row_id);
//         if (value) {
//           if constexpr (!IS_STRING_COLUMN) {
//             column_normalized_values.push_back(static_cast<long long>((value).value()));
//           } else {
//             column_normalized_values.push_back(static_cast<long long>(hash_function((value).value())));
//           }
//           column_null_values.push_back(0);
//         } else {
//           column_normalized_values.push_back(0.0);
//           column_null_values.push_back(1);
//         }
//       }
//      });
//     normalized_values.push_back(std::move(column_normalized_values));
//     null_values.push_back(std::move(column_null_values));
//   }


//   for (size_t row_id = 0; row_id < row_count; row_id++) {  
//     std::pair<std::vector<long long>, std::vector<size_t>> row_determinant{std::vector<long long>(),
//                                                                               std::vector<size_t>()};
//     std::pair<std::vector<long long>, std::vector<size_t>> row_dependent{std::vector<long long>(),
//                                                                               std::vector<size_t>()};
//     for (size_t i = 0; i < determinant.size(); i++) {
//       row_determinant.first.push_back(normalized_values[i][row_id]);
//       row_determinant.second.push_back(null_values[i][row_id]);
//     }
//     for (size_t i = determinant.size(); i < (determinant.size() + dependent.size()) ; i++) {
//       row_dependent.first.push_back(normalized_values[i][row_id]);
//       row_dependent.second.push_back(null_values[i][row_id]);
//     }
//     if (dependency.contains(row_determinant)) {
//       if (dependency[row_determinant] != row_dependent) {
//         return false;
//       }
//     } else {
//       dependency[row_determinant] = row_dependent;
//     }
//   }
//   return true;
// }


// bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table,
//                                                    const std::vector<ColumnID> determinant,
//                                                    const std::vector<ColumnID> dependent) {
//   // 1. iterate over determinant
//   // 2. Check if determinant as key exists. If not add it.
//   // 3. Check if value is the same as determinant. If not break.

//   struct Hasher {
//     size_t operator()(const std::pair<std::vector<unsigned long long>, std::vector<size_t>> attributes) const {
//       auto seed = size_t{0};
//       for (auto& attribut : attributes.first) {
//         boost::hash_combine(seed, attribut);
//       }
//       for (auto& attribut : attributes.second) {
//         boost::hash_combine(seed, attribut);
//       }
//       return seed;
//     }
//   };

//   const auto row_count = table->row_count();

//   std::unordered_map<std::pair<std::vector<unsigned long long>, std::vector<size_t>>,
//                      std::pair<std::vector<unsigned long long>, std::vector<size_t>>, Hasher>
//       dependency;
//   for (size_t row_id = 0; row_id < row_count; row_id++) {
//     // Inside the first object, we need to save the values of the attributes. If the value is null we save 0.
//     // Inside the second object, we need to keep track of which value is null. That way every value combination of a
//     // row gets a unique Pair. This is important to be able to hash correctly:
//     // {a | null | 0 } -> {Tom} has the pair <[a, 0, 0], [0, 1, 0]>
//     // {a | 0 | null } -> {Alex} has the pair <[a, 0, 0], [0, 0, 1]>
//     // We are keeping track of the null rows so that we do not generate the same hash for two different attribute
//     // values with the value 0 and the value null.
//     std::pair<std::vector<unsigned long long>, std::vector<size_t>> row_determinant{std::vector<unsigned long long>(),
//                                                                                 std::vector<size_t>()};
//     auto hash_function = std::hash<pmr_string>{};
//     for (const auto column_id : determinant) {
//       resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
//         using ColumnDataType = typename decltype(data_type_t)::type;
//         auto value = table->get_value<ColumnDataType>(column_id, row_id);
//         constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
//         if (value) {
//           if constexpr (!IS_STRING_COLUMN) {
//             row_determinant.first.push_back(static_cast<unsigned long long>((value).value()));
//           } else {
//             row_determinant.first.push_back(static_cast<unsigned long long>(hash_function((value).value())));
//           }
//           row_determinant.second.push_back(0);
//         } else {
//           row_determinant.first.push_back(0.0);
//           row_determinant.second.push_back(1);
//         }
//       });
//     }
//     std::pair<std::vector<unsigned long long>, std::vector<size_t>> row_dependent{std::vector<unsigned long long>(),
//                                                                               std::vector<size_t>()};
//     for (const auto column_id : dependent) {
//       resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
//         using ColumnDataType = typename decltype(data_type_t)::type;
//         auto value = table->get_value<ColumnDataType>(column_id, row_id);
//         constexpr auto IS_STRING_COLUMN = (std::is_same<ColumnDataType, pmr_string>{});
//         if (value) {
//           if constexpr (!IS_STRING_COLUMN) {
//             row_dependent.first.push_back(static_cast<unsigned long long>((value).value()));
//           } else {
//             row_dependent.first.push_back(static_cast<unsigned long long>(hash_function((value).value())));
//           }
//           row_dependent.second.push_back(0);
//         } else {
//           row_dependent.first.push_back(0.0);
//           row_dependent.second.push_back(1);
//         }
//       });
//     }
//     if (dependency.contains(row_determinant)) {
//       if (dependency[row_determinant] != row_dependent) {
//         return false;
//       }
//     } else {
//       dependency[row_determinant] = row_dependent;
//     }
//   }
//   return true;
// }

EXPORT_PLUGIN(FunctionalDependencyPlugin)

}  // namespace opossum