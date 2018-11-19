#include "tpch_query_generator.hpp"

#include <numeric>

#include "tpch_queries.hpp"
#include "utils/assert.hpp"

namespace opossum {

TPCHQueryGenerator::TPCHQueryGenerator() {
  _generate_names();
  _selected_queries.resize(22);
  std::iota(_selected_queries.begin(), _selected_queries.end(), QueryID{0});
  _generate_preparation_queries();
}

TPCHQueryGenerator::TPCHQueryGenerator(const std::vector<QueryID>& selected_queries) {
  _generate_names();
  _selected_queries = selected_queries;
  _generate_preparation_queries();
}

void TPCHQueryGenerator::_generate_names() {
  _query_names.reserve(22);
  for (auto i = 0; i < 22; ++i) {
    _query_names.emplace_back(std::string("TPC-H ") + std::to_string(i + 1));
  }
}

void TPCHQueryGenerator::_generate_preparation_queries() {
  for (const auto& query_id : _selected_queries) {
    _preparation_queries.emplace_back(tpch_queries.find(query_id + 1)->second);
  }
}

std::string TPCHQueryGenerator::build_query(const QueryID query_id) {
  DebugAssert(query_id < 22, "There are only 22 TPC-H queries");

  static std::vector<std::string> execute_statements = {
      "EXECUTE TPCH1  ('1998-09-02');",
      "EXECUTE TPCH2  (15, '%BRASS', 'EUROPE', 'EUROPE');",
      "EXECUTE TPCH3  ('BUILDING', '1995-03-15', '1995-03-15');",
      "EXECUTE TPCH4  ('1996-07-01', '1996-10-01');",
      "EXECUTE TPCH5  ('AMERICA', '1994-01-01', '1995-01-01');",
      "EXECUTE TPCH6  ();",
      "EXECUTE TPCH7  ('IRAN', 'IRAQ', 'IRAQ', 'IRAN');",
      "EXECUTE TPCH8  ('BRAZIL', 'AMERICA', 'ECONOMY ANODIZED STEEL');",
      "EXECUTE TPCH9  ('%green%');",
      "EXECUTE TPCH10 ('1993-10-01', '1994-01-01');",
      "EXECUTE TPCH11 ('GERMANY', 0.0001, 'GERMANY');",
      "EXECUTE TPCH12 ('MAIL', 'SHIP', '1994-01-01', '1995-01-01');",
      "EXECUTE TPCH13 ();",
      "EXECUTE TPCH14 ('1995-09-01', '1995-10-01');",
      "EXECUTE TPCH15a ('1993-05-13', '1993-08-13'); EXECUTE TPCH15b (); EXECUTE TPCH15c;",
      "EXECUTE TPCH16 ('Brand#45', 'MEDIUM POLISHED%', 49, 14, 23, 45, 19, 3, 36, 9);",
      "EXECUTE TPCH17 ('Brand#23', 'MED BOX');",
      "EXECUTE TPCH18 (300);",
      "EXECUTE TPCH19 ('Brand#12', 1, 1, 'Brand#23', 10, 10, 'Brand#34', 20, 20);",
      "EXECUTE TPCH20 ('forest%', '1995-01-01', '1994-01-01', 'CANADA');",
      "EXECUTE TPCH21 ('SAUDI ARABIA');",
      "EXECUTE TPCH22 ();"};

  if (!execute_statements[query_id].empty()) return execute_statements[query_id];

  return tpch_queries.find(query_id + 1)->second;
}

}  // namespace opossum
