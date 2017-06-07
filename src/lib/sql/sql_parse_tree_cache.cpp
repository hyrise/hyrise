#include "sql_parse_tree_cache.hpp"

using hsql::SQLParserResult;

namespace opossum {

SQLParseTreeCache::SQLParseTreeCache(size_t capacity) : _cache(capacity) {}

SQLParseTreeCache::~SQLParseTreeCache() {}

void SQLParseTreeCache::set(const std::string& query, std::shared_ptr<SQLParserResult> result) {
  if (_cache.capacity() == 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(_mutex);
  _cache.set(query, result);
}

bool SQLParseTreeCache::has(const std::string& query) const {
  if (_cache.capacity() == 0) {
    return false;
  }

  return _cache.has(query);
}

std::shared_ptr<SQLParserResult> SQLParseTreeCache::get(const std::string& query) {
  std::lock_guard<std::mutex> lock(_mutex);
  return _cache.get(query);
}

void SQLParseTreeCache::reset(size_t capacity) {
  _cache = LRUCache<std::string, std::shared_ptr<hsql::SQLParserResult>>(capacity);
}

}  // namespace opossum
