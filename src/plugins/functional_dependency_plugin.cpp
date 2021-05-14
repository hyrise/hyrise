#include <unordered_map>
#include <utility>
#include <chrono>
#include <iostream>

#include <boost/functional/hash_fwd.hpp>

#include "all_type_variant.hpp"
#include "functional_dependency_plugin.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "types.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

std::string FunctionalDependencyPlugin::description() const { return "Check for functional dependencies"; }

void FunctionalDependencyPlugin::start() {
  // std::cout << "Loading table sf-1" << std::endl;
  // const auto& table = BinaryParser::parse("cmake-build-release/tpch_cached_tables/sf-1.000000/customer.bin");
  // std::cout << "Table loaded sf-1" << std::endl;
  // std::vector<ColumnID> determinant{};
  // determinant.push_back(table->column_id_by_name("c_custkey"));
  // std::vector<ColumnID> dependent{};
  // dependent.push_back(table->column_id_by_name("c_name"));
  // dependent.push_back(table->column_id_by_name("c_acctbal"));
  // dependent.push_back(table->column_id_by_name("c_phone"));
  // dependent.push_back(table->column_id_by_name("c_address"));
  // dependent.push_back(table->column_id_by_name("c_comment"));
  // auto start = std::chrono::high_resolution_clock::now();
  // auto dependency = _check_dependency(table, determinant, dependent);
  // auto end = std::chrono::high_resolution_clock::now();
  // auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  // std::cout << "Sf-1 dependency: " << dependency << std::endl;
  // std::cout << "Execution time sf-1: " << ms_int.count() << std::endl;
  // std::cout << "Loading table sf-10" << std::endl;
  // const auto& table_sf_10 = BinaryParser::parse("cmake-build-release/tpch_cached_tables/sf-10.000000/customer.bin");
  // std::cout << "Table loaded sf-10" << std::endl;
  // std::vector<ColumnID> determinant_sf_10{};
  // determinant_sf_10.push_back(table_sf_10->column_id_by_name("c_custkey"));
  // std::vector<ColumnID> dependent_sf_10{};
  // dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_name"));
  // dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_acctbal"));
  // dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_phone"));
  // dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_address"));
  // dependent_sf_10.push_back(table_sf_10->column_id_by_name("c_comment"));
  // start = std::chrono::high_resolution_clock::now();
  // dependency = _check_dependency(table_sf_10, determinant_sf_10, dependent_sf_10);
  // end = std::chrono::high_resolution_clock::now();
  // ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  // std::cout << "Sf-10 dependency: " << dependency << std::endl;
  // std::cout << "Execution time sf-10: " << ms_int.count() << std::endl;
}

void FunctionalDependencyPlugin::stop() {}

bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table,
                                                   const std::vector<ColumnID> determinant,
                                                   const std::vector<ColumnID> dependent) {
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

  std::unordered_map<std::vector<AllTypeVariant>, std::vector<AllTypeVariant>, Hasher> dependency;

  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id++) {
    const auto chunk = table->get_chunk(chunk_id);
    std::vector<std::shared_ptr<AbstractSegment>> segments_determinant;
    std::vector<std::shared_ptr<AbstractSegment>> segments_dependent;
    for (const auto column_id : determinant) {
      segments_determinant.push_back(chunk->get_segment(column_id));
    }
    for (const auto column_id : dependent) {
      segments_dependent.push_back(chunk->get_segment(column_id));
    }
    // TODO: Check that there are segments/determinants/dependents at all 
    const auto segment_size = segments_determinant[0]->size();
    for (auto segment_row = ChunkOffset{0}; segment_row < segment_size; segment_row++) {
      std::vector<AllTypeVariant> row_determinant;
      std::vector<AllTypeVariant> row_dependent;
      for (auto segment : segments_determinant) {
        row_determinant.push_back((*segment)[segment_row]);
      }
      for (auto segment : segments_dependent) {
        row_dependent.push_back((*segment)[segment_row]);
      }
      if (dependency.contains(row_determinant)) {
        if (dependency[row_determinant] != row_dependent) {
          return false;
        }
      } else {
        dependency[row_determinant] = row_dependent;
      }
    }
  }
  return true;                                                                       
}

EXPORT_PLUGIN(FunctionalDependencyPlugin)

}  // namespace opossum