#include <SQLParser.h>
#include <iostream>
#include <sql/sql_pipeline_statement.hpp>

int main() {
  using namespace opossum;

  const std::string sql = "SELECT * FROM foo;\n\t\t\tSELECT * FROM bla;\nSELECT * FROM foo";
  const auto tpch = R"(
SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, "part"
WHERE p_partkey = ps_partkey
      AND p_brand <> 'Brand#45'
      AND p_type not like 'MEDIUM POLISHED%'
      AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
      AN ps_suppkey not in
          (SELECT s_suppkey
           FROM supplier
           WHERE s_comment like '%Customer%Complaints%'
      )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;)";

  auto x = SQLPipelineStatement::from_sql_string(sql);
  auto y = SQLPipelineStatement::from_sql_string(tpch);

  std::cout << x.size() << y.size() << std::endl;

  return 0;
}
