#include "sql_parse_tree_cache.hpp"

using hsql::SQLParserResult;

namespace opossum {

SQLParseTreeCache::SQLParseTreeCache(size_t capacity) : _cache(capacity) {}

SQLParseTreeCache::~SQLParseTreeCache() {}

void SQLParseTreeCache::set(const std::string& query, std::shared_ptr<SQLParserResult> result) {
  if (_cache.capacity() == 0) {
    return;
  }

  _cache.set(query, result);
}

bool SQLParseTreeCache::has(const std::string& query) { return _cache.has(query); }

std::shared_ptr<SQLParserResult> SQLParseTreeCache::get(const std::string& query) { return _cache.get(query); }

}  // namespace opossum
