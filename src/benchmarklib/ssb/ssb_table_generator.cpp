#include "ssb_table_generator.hpp"

#include "external_dbgen_utils.hpp"
#include "storage/table.hpp"

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
  // Set all primary (PK) and foreign keys (FK) as defined in the specification (2. Detail on SSB Format, p. 2-4).

  // Get all tables.
  const auto& lineorder_table = table_info_by_name.at("lineorder").table;
  const auto& part_table = table_info_by_name.at("part").table;
  const auto& supplier_table = table_info_by_name.at("supplier").table;
  const auto& customer_table = table_info_by_name.at("customer").table;
  const auto& date_table = table_info_by_name.at("date").table;

  // Set constraints.

  // lineorder - 1 composite PK, 5 FKs.
  lineorder_table->add_soft_constraint(TableKeyConstraint{
      {lineorder_table->column_id_by_name("lo_orderkey"), lineorder_table->column_id_by_name("lo_linenumber")},
      KeyConstraintType::PRIMARY_KEY});
  lineorder_table->add_soft_constraint(ForeignKeyConstraint{{lineorder_table->column_id_by_name("lo_custkey")},
                                                            lineorder_table,
                                                            {customer_table->column_id_by_name("c_custkey")},
                                                            customer_table});
  lineorder_table->add_soft_constraint(ForeignKeyConstraint{{lineorder_table->column_id_by_name("lo_partkey")},
                                                            lineorder_table,
                                                            {part_table->column_id_by_name("p_partkey")},
                                                            part_table});
  lineorder_table->add_soft_constraint(ForeignKeyConstraint{{lineorder_table->column_id_by_name("lo_suppkey")},
                                                            lineorder_table,
                                                            {supplier_table->column_id_by_name("s_suppkey")},
                                                            supplier_table});
  lineorder_table->add_soft_constraint(ForeignKeyConstraint{{lineorder_table->column_id_by_name("lo_orderdate")},
                                                            lineorder_table,
                                                            {date_table->column_id_by_name("d_datekey")},
                                                            date_table});
  lineorder_table->add_soft_constraint(ForeignKeyConstraint{{lineorder_table->column_id_by_name("lo_commitdate")},
                                                            lineorder_table,
                                                            {date_table->column_id_by_name("d_datekey")},
                                                            date_table});

  // part - 1 PK.
  part_table->add_soft_constraint(
      TableKeyConstraint{{part_table->column_id_by_name("p_partkey")}, KeyConstraintType::PRIMARY_KEY});

  // supplier - 1 PK.
  supplier_table->add_soft_constraint(
      TableKeyConstraint{{supplier_table->column_id_by_name("s_suppkey")}, KeyConstraintType::PRIMARY_KEY});

  // customer - 1 PK.
  customer_table->add_soft_constraint(
      TableKeyConstraint{{customer_table->column_id_by_name("c_custkey")}, KeyConstraintType::PRIMARY_KEY});

  // date - 1 PK.
  date_table->add_soft_constraint(
      TableKeyConstraint{{date_table->column_id_by_name("d_datekey")}, KeyConstraintType::PRIMARY_KEY});
}

}  // namespace hyrise
