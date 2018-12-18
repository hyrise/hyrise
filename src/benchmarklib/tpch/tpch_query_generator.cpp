#include "tpch_query_generator.hpp"

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <numeric>
#include <sstream>

#include "tpch_queries.hpp"
#include "utils/assert.hpp"

namespace opossum {

TPCHQueryGenerator::TPCHQueryGenerator(bool use_prepared_statements)
    : _use_prepared_statements(use_prepared_statements) {
  _generate_names();
  _selected_queries.resize(22);
  std::iota(_selected_queries.begin(), _selected_queries.end(), QueryID{0});
}

TPCHQueryGenerator::TPCHQueryGenerator(bool use_prepared_statements, const std::vector<QueryID>& selected_queries)
    : _use_prepared_statements(use_prepared_statements) {
  _generate_names();
  _selected_queries = selected_queries;
}

void TPCHQueryGenerator::_generate_names() {
  _query_names.reserve(22);
  for (auto i = 0; i < 22; ++i) {
    _query_names.emplace_back(std::string("TPC-H ") + std::to_string(i + 1));
  }
}

std::string TPCHQueryGenerator::get_preparation_queries() const {
  if (!_use_prepared_statements) return "";

  std::stringstream sql;
  for (auto query_id = QueryID{0}; query_id < 22; ++query_id) {
    if (query_id + 1 == 15) {
      // We cannot prepare query 15, because the SELECT relies on a view that is generated in the first step. We'll have
      // to manually build this query once we start randomizing the parameters.
      continue;
    }

    auto query_template = std::string{tpch_queries.find(query_id + 1)->second};

    // Escape single quotes
    boost::replace_all(query_template, "'", "''");

    sql << "PREPARE TPCH" << (query_id + 1) << " FROM '" << query_template << "';\n";
  }
  return sql.str();
}

std::string TPCHQueryGenerator::build_query(const QueryID query_id) {
  DebugAssert(query_id < 22, "There are only 22 TPC-H queries");

  if (query_id + 1 == 15) {
    // Generating TPC-H Query 15 by hand
    auto query_15 = std::string{tpch_queries.find(15)->second};

    // TPC-H query 15 uses "stream ids" to name the views. While not supported right now, we might want to execute
    // multiple instances of Q15 simultaneously and will need unique view names for that.
    static auto view_id = 0;
    boost::replace_all(query_15, std::string("revenueview"), std::string("revenue") + std::to_string(view_id++));
    return query_15;
  }

  // Stores how the parameters (the ? in the query) should be replaced. These values are examples for the queries. Most
  // of them use the validation parameters given in the TPC-H specification for the respective query. A few are
  // modified so that we get results even for a small scale factor.
  static std::vector<std::vector<std::string>> parameter_values = {
      {"'1998-09-02'"},
      {"15", "'%BRASS'", "'EUROPE'", "'EUROPE'"},
      {"'BUILDING'", "'1995-03-15'", "'1995-03-15'"},
      {"'1993-07-01'", "'1993-10-01'"},
      {"'ASIA'", "'1994-01-01'", "'1995-01-01'"},
      {"'1994-01-01'", "'1995-01-01'", ".06", ".06", "24"},
      {"'FRANCE'", "'GERMANY'", "'GERMANY'", "'FRANCE'"},
      {"'BRAZIL'", "'AMERICA'", "'ECONOMY ANODIZED STEEL'"},
      {"'%green%'"},
      {"'1993-10-01'", "'1994-01-01'"},
      {"'GERMANY'", "0.0001", "'GERMANY'"},
      {"'MAIL'", "'SHIP'", "'1994-01-01'", "'1995-01-01'"},
      {"'%special%requests%'"},
      {"'1995-09-01'", "'1995-10-01'"},
      {},
      {"'Brand#45'", "'MEDIUM POLISHED%'", "49", "14", "23", "45", "19", "3", "36", "9"},
      {"'Brand#23'", "'MED BOX'"},
      {"300"},
      {"'Brand#12'", "1", "1", "'Brand#23'", "10", "10", "'Brand#34'", "20", "20"},
      {"'forest%'", "'1994-01-01'", "'1995-01-01'", "'CANADA'"},
      {"'SAUDI ARABIA'"},
      {"'13'", "'31'", "'23'", "'29'", "'30'", "'18'", "'17'", "'13'", "'31'", "'23'", "'29'", "'30'", "'18'", "'17'"}};

  return _build_executable_query(query_id, parameter_values[query_id]);
}

std::string TPCHQueryGenerator::_build_executable_query(const QueryID query_id,
                                                        const std::vector<std::string>& parameter_values) {
  if (_use_prepared_statements) {
    // Join the parameter values for an "EXECUTE TPCHn VALUES (...)" string
    std::stringstream sql;
    sql << "EXECUTE TPCH" << (query_id + 1) << " (" << boost::algorithm::join(parameter_values, ", ") << ")";
    return sql.str();
  } else {
    // Take the SQL query (from tpch_queries.cpp) and replace one placeholder (question mark) after another
    auto query_template = std::string{tpch_queries.find(query_id + 1)->second};

    for (const auto& parameter_value : parameter_values) {
      boost::replace_first(query_template, "?", parameter_value);
    }

    return query_template;
  }
}

}  // namespace opossum
