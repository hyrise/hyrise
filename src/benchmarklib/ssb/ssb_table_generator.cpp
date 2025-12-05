#include "ssb_table_generator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "external_dbgen_utils.hpp"
#include "file_based_table_generator.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/table.hpp"  // IWYU pragma: keep
#include "types.hpp"

namespace hyrise {

const auto ssb_table_names = std::vector<std::string>{"part", "customer", "supplier", "date", "lineorder"};

SSBTableGenerator::SSBTableGenerator(const std::string& dbgen_path, const std::string& csv_meta_path,
                                     const std::string& data_path, float scale_factor, ChunkOffset chunk_size)
    : SSBTableGenerator(dbgen_path, csv_meta_path, data_path, scale_factor,
                        std::make_shared<BenchmarkConfig>(chunk_size)) {}

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
  primary_key_constraint(lineorder_table, {"lo_orderkey", "lo_linenumber"});
  foreign_key_constraint(lineorder_table, {"lo_custkey"}, customer_table, {"c_custkey"});
  foreign_key_constraint(lineorder_table, {"lo_partkey"}, part_table, {"p_partkey"});
  foreign_key_constraint(lineorder_table, {"lo_suppkey"}, supplier_table, {"s_suppkey"});
  foreign_key_constraint(lineorder_table, {"lo_orderdate"}, date_table, {"d_datekey"});
  foreign_key_constraint(lineorder_table, {"lo_commitdate"}, date_table, {"d_datekey"});

  // part - 1 PK.
  primary_key_constraint(part_table, {"p_partkey"});

  // supplier - 1 PK.
  primary_key_constraint(supplier_table, {"s_suppkey"});

  // customer - 1 PK.
  primary_key_constraint(customer_table, {"c_custkey"});

  // date - 1 PK.
  primary_key_constraint(date_table, {"d_datekey"});

  unique_constraint(date_table, {"d_datekey"});

  
  unique_constraint(date_table, {"d_date"});
  unique_constraint(date_table, {"d_daynuminyear","d_yearmonthnum"});
  unique_constraint(date_table, {"d_daynuminyear","d_yearmonth"});
  unique_constraint(date_table, {"d_daynuminyear","d_year"});
  unique_constraint(date_table, {"d_weeknuminyear","d_dayofweek","d_yearmonthnum"});
  unique_constraint(date_table, {"d_weeknuminyear","d_yearmonthnum","d_daynuminweek"});
  unique_constraint(date_table, {"d_yearmonthnum","d_daynuminmonth"});
  unique_constraint(date_table, {"d_weeknuminyear","d_dayofweek","d_yearmonth"});
  unique_constraint(date_table, {"d_weeknuminyear","d_yearmonth","d_daynuminweek"});
  unique_constraint(date_table, {"d_yearmonth","d_daynuminmonth"});
  unique_constraint(date_table, {"d_weeknuminyear","d_year","d_daynuminmonth"});
  unique_constraint(date_table, {"d_weeknuminyear","d_dayofweek","d_year"});
  unique_constraint(date_table, {"d_weeknuminyear","d_year","d_daynuminweek"});
  unique_constraint(date_table, {"d_month","d_year","d_daynuminmonth"});
  unique_constraint(date_table, {"d_monthnuminyear","d_year","d_daynuminmonth"});
  unique_constraint(part_table, {"p_partkey"});
  unique_constraint(supplier_table, {"s_suppkey"});
  unique_constraint(supplier_table, {"s_name"});
  unique_constraint(supplier_table, {"s_address"});
  unique_constraint(supplier_table, {"s_phone"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_revenue","lo_partkey"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_revenue","lo_commitdate"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_revenue","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_revenue","lo_tax"});
  unique_constraint(lineorder_table, {"lo_ordtotalprice","lo_revenue","lo_commitdate"});
  unique_constraint(lineorder_table, {"lo_revenue","lo_commitdate","lo_custkey"});
  unique_constraint(lineorder_table, {"lo_revenue","lo_custkey","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_revenue","lo_suppkey","lo_orderdate"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_extendedprice","lo_commitdate"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_extendedprice","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_partkey","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_supplycost","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_orderkey","lo_linenumber"});
  unique_constraint(lineorder_table, {"lo_extendedprice","lo_ordtotalprice","lo_commitdate"});
  unique_constraint(lineorder_table, {"lo_extendedprice","lo_ordtotalprice","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_ordtotalprice","lo_partkey","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_ordtotalprice","lo_supplycost","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_extendedprice","lo_partkey","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_extendedprice","lo_commitdate","lo_custkey"});
  unique_constraint(lineorder_table, {"lo_extendedprice","lo_custkey","lo_suppkey"});
  unique_constraint(lineorder_table, {"lo_partkey","lo_suppkey","lo_quantity"});
  unique_constraint(customer_table, {"c_custkey"});
  unique_constraint(customer_table, {"c_name"});
  unique_constraint(customer_table, {"c_address"});
  unique_constraint(customer_table, {"c_phone"});


}

}  // namespace hyrise
