#include "tpch_query_generator.hpp"

#include <boost/algorithm/string/replace.hpp>
#include <numeric>
#include <sstream>

#include "tpch_queries.hpp"
#include "utils/assert.hpp"

namespace opossum {

TPCHQueryGenerator::TPCHQueryGenerator() {
  _generate_names();
  _selected_queries.resize(22);
  std::iota(_selected_queries.begin(), _selected_queries.end(), QueryID{0});
}

TPCHQueryGenerator::TPCHQueryGenerator(const std::vector<QueryID>& selected_queries) {
  _generate_names();
  _selected_queries = selected_queries;
}

void TPCHQueryGenerator::_generate_names() {
  _query_names.reserve(22);
  for (auto i = 0; i < 22; ++i) {
    _query_names.emplace_back(std::string("TPC-H ") + std::to_string(i + 1));
  }
}

std::string TPCHQueryGenerator::setup_queries() const {
  std::stringstream sql;
  for (auto query_id = QueryID{0}; query_id < 22; ++query_id) {
    if (query_id + 1 == 15) {
      // We cannot prepare query 15, because the SELECT relies on a view that is generated in the first step. We'll have
      // to manually build this query once we start randomizing the parameters.
      continue;
    }
    sql << tpch_queries.find(query_id + 1)->second;
  }
  return sql.str();
}

std::string TPCHQueryGenerator::build_query(const QueryID query_id) {
  DebugAssert(query_id < 22, "There are only 22 TPC-H queries");

  if (query_id == 14) {
    // Generating TPC-H Query 15 by hand
    auto query_15 = std::string{tpch_queries.find(15)->second};

    static auto view_id = 0;
    boost::replace_all(query_15, std::string("revenueview"), std::string("revenue") + std::to_string(view_id++));
    // TODO(anyone): Set random parameters
    return query_15;
  }

  static std::vector<std::string> execute_statements = {
      "EXECUTE TPCH1  ('1998-09-02');",
      "EXECUTE TPCH2  (15, '%BRASS', 'EUROPE', 'EUROPE');",
      "EXECUTE TPCH3  ('BUILDING', '1995-03-15', '1995-03-15');",
      "EXECUTE TPCH4  ('1996-07-01', '1996-10-01');",
      "EXECUTE TPCH5  ('AMERICA', '1994-01-01', '1995-01-01');",
      "EXECUTE TPCH6  ('1994-01-01', '1995-01-01', .06, .06, 24);",
      "EXECUTE TPCH7  ('IRAN', 'IRAQ', 'IRAQ', 'IRAN');",
      "EXECUTE TPCH8  ('BRAZIL', 'AMERICA', 'ECONOMY ANODIZED STEEL');",
      "EXECUTE TPCH9  ('%green%');",
      "EXECUTE TPCH10 ('1993-10-01', '1994-01-01');",
      "EXECUTE TPCH11 ('GERMANY', 0.0001, 'GERMANY');",
      "EXECUTE TPCH12 ('MAIL', 'SHIP', '1994-01-01', '1995-01-01');",
      "EXECUTE TPCH13 ('%special%requests%');",
      "EXECUTE TPCH14 ('1995-09-01', '1995-10-01');",
      "",  // see comment in _generate_preparation_queries.
      "EXECUTE TPCH16 ('Brand#45', 'MEDIUM POLISHED%', 49, 14, 23, 45, 19, 3, 36, 9);",
      "EXECUTE TPCH17 ('Brand#23', 'MED BOX');",
      "EXECUTE TPCH18 (300);",
      "EXECUTE TPCH19 ('Brand#12', 1, 1, 'Brand#23', 10, 10, 'Brand#34', 20, 20);",
      "EXECUTE TPCH20 ('forest%', '1995-01-01', '1994-01-01', 'CANADA');",
      "EXECUTE TPCH21 ('SAUDI ARABIA');",
      "EXECUTE TPCH22 ('13', '31', '23', '29', '30', '18', '17');"};

  if (!execute_statements[query_id].empty()) return execute_statements[query_id];

  return tpch_queries.find(query_id + 1)->second;
}

}  // namespace opossum
