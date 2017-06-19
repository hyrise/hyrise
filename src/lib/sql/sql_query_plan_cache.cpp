#include "sql_query_plan_cache.hpp"

#include <memory>
#include <string>

namespace opossum {

SQLQueryPlanCache::SQLQueryPlanCache(size_t capacity) : _cache(capacity) {}

SQLQueryPlanCache::~SQLQueryPlanCache() {}

void SQLQueryPlanCache::set(const std::string& query, SQLQueryPlan result) {
  if (_cache.capacity() == 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(_mutex);
  _cache.set(query, result);
}

bool SQLQueryPlanCache::try_get(const std::string& query, SQLQueryPlan* result) {
  if (_cache.capacity() == 0) {
    return false;
  }

  std::lock_guard<std::mutex> lock(_mutex);
  if (!_cache.has(query)) {
    return false;
  }
  *result = _cache.get(query);
  return true;
}

bool SQLQueryPlanCache::has(const std::string& query) const { return _cache.has(query); }

SQLQueryPlan SQLQueryPlanCache::get(const std::string& query) {
  std::lock_guard<std::mutex> lock(_mutex);
  return _cache.get(query);
}

void SQLQueryPlanCache::clear_and_resize(size_t capacity) { _cache = LRUCache<std::string, SQLQueryPlan>(capacity); }

void SQLQueryPlanCache::clear() {
  size_t capacity = _cache.capacity();
  _cache = LRUCache<std::string, SQLQueryPlan>(capacity);
}

}  // namespace opossum
