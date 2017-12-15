#include "system_statistics.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"

namespace opossum {

SystemStatistics::SystemStatistics() : _recent_queries{} {}

const std::vector<SQLQueryPlan>& SystemStatistics::recent_queries() const {
  // TODO(group01) lazily initialize this and update only if there were changes
  _recent_queries.clear();

  // TODO(group01) introduce values() method in AbstractCache interface and implement in all subclasses
  const auto& query_plan_cache = SQLQueryOperator::get_query_plan_cache().cache();
  // TODO(group01) implement for cache implementations other than GDFS cache
  auto gdfs_cache_ptr = dynamic_cast<const GDFSCache<std::string, SQLQueryPlan>*>(&query_plan_cache);
  Assert(gdfs_cache_ptr, "Expected GDFS Cache");

  const boost::heap::fibonacci_heap<GDFSCache<std::string, SQLQueryPlan>::GDFSCacheEntry>& fibonacci_heap =
      gdfs_cache_ptr->queue();
  std::cout << "Query plan cache size: " << fibonacci_heap.size() << "\n";
  auto cache_iterator = fibonacci_heap.ordered_begin();
  auto cache_end = fibonacci_heap.ordered_end();

  for (; cache_iterator != cache_end; ++cache_iterator) {
    const GDFSCache<std::string, SQLQueryPlan>::GDFSCacheEntry& query_plan = *cache_iterator;
    _recent_queries.push_back(query_plan.value);
  }

  return _recent_queries;
}

}  // namespace opossum
