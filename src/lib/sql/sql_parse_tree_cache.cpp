#include "sql_parse_tree_cache.hpp"

#include <memory>
#include <string>

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

bool SQLParseTreeCache::try_get(const std::string& query, std::shared_ptr<hsql::SQLParserResult>* result) {
  std::lock_guard<std::mutex> lock(_mutex);
  if (!_cache.has(query)) {
    return false;
  }
  *result = _cache.get(query);
  return true;
}

bool SQLParseTreeCache::has(const std::string& query) const { return _cache.has(query); }

std::shared_ptr<SQLParserResult> SQLParseTreeCache::get(const std::string& query) {
  std::lock_guard<std::mutex> lock(_mutex);
  return _cache.get(query);
}

void SQLParseTreeCache::clear_and_resize(size_t capacity) {
  _cache = LRUCache<std::string, std::shared_ptr<SQLParserResult>>(capacity);
}

void SQLParseTreeCache::clear() {
  size_t capacity = _cache.capacity();
  _cache = LRUCache<std::string, std::shared_ptr<SQLParserResult>>(capacity);
}

}  // namespace opossum
