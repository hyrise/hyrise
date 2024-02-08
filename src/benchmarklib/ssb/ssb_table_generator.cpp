#include "ssb_table_generator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "external_dbgen_utils.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

const auto ssb_table_names = std::vector<std::string>{"part", "customer", "supplier", "date", "lineorder"};

SSBTableGenerator::SSBTableGenerator(const std::string& dbgen_path, const std::string& csv_meta_path,
                                     const std::string& data_path, float scale_factor, ChunkOffset chunk_size)
    : SSBTableGenerator(dbgen_path, csv_meta_path, data_path, scale_factor,
                        create_benchmark_config_with_chunk_size(chunk_size)) {}

SSBTableGenerator::SSBTableGenerator(const std::string& dbgen_path, const std::string& csv_meta_path,
                                     const std::string& data_path, float scale_factor,
                                     const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator{benchmark_config},
      FileBasedTableGenerator{benchmark_config, data_path + "/"},
      _dbgen_path{dbgen_path},
      _csv_meta_path{csv_meta_path},
      _scale_factor{scale_factor} {}

std::unordered_map<std::string, BenchmarkTableInfo> SSBTableGenerator::generate() {
  generate_csv_tables_with_external_dbgen(_dbgen_path, ssb_table_names, _csv_meta_path, _path, _scale_factor, "-T a");

  // Having generated the .csv files, call the FileBasedTableGenerator just as if those files were user-provided.
  const auto& generated_tables = FileBasedTableGenerator::generate();

  // FileBasedTableGenerator automatically stores a binary file. Remove the CSV data to save some space.
  remove_csv_tables(_path);

  return generated_tables;
}

void SSBTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {
  const auto& customer_table = table_info_by_name.at("customer").table;
  customer_table->add_soft_key_constraint(
      {{customer_table->column_id_by_name("c_custkey")}, KeyConstraintType::PRIMARY_KEY});

  const auto& lineorder_table = table_info_by_name.at("lineorder").table;
  const auto lineorder_pk_constraint = TableKeyConstraint{
      {lineorder_table->column_id_by_name("lo_orderkey"), lineorder_table->column_id_by_name("lo_linenumber")},
      KeyConstraintType::PRIMARY_KEY};
  lineorder_table->add_soft_key_constraint(lineorder_pk_constraint);

  const auto& part_table = table_info_by_name.at("part").table;
  const auto part_table_pk_constraint =
      TableKeyConstraint{{part_table->column_id_by_name("p_partkey")}, KeyConstraintType::PRIMARY_KEY};
  part_table->add_soft_key_constraint(part_table_pk_constraint);

  const auto& supplier_table = table_info_by_name.at("supplier").table;
  const auto supplier_pk_constraint =
      TableKeyConstraint{{supplier_table->column_id_by_name("s_suppkey")}, KeyConstraintType::PRIMARY_KEY};
  supplier_table->add_soft_key_constraint(supplier_pk_constraint);

  const auto& date_table = table_info_by_name.at("date").table;
  const auto date_pk_constraint =
      TableKeyConstraint{{date_table->column_id_by_name("d_datekey")}, KeyConstraintType::PRIMARY_KEY};
  date_table->add_soft_key_constraint(date_pk_constraint);
}

}  // namespace hyrise
