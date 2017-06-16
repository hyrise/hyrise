#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "lru_cache.hpp"

#include "SQLParserResult.h"

namespace opossum {

// Cache that stores instances of SQLParserResult.
// Uses the least-recently-used cache as underlying storage.
class SQLParseTreeCache {
 public:
  explicit SQLParseTreeCache(size_t capacity);

  virtual ~SQLParseTreeCache();

  // Adds or refreshes the cache entry [query, result].
  void set(const std::string& query, std::shared_ptr<hsql::SQLParserResult> result);

  // Tries to fetch the cache entry for the query into the result object.
  // Returns true if the entry was found, false otherwise.
  bool try_get(const std::string& query, std::shared_ptr<hsql::SQLParserResult>* result);

  // Checks whether an entry for the query exists.
  bool has(const std::string& query) const;

  // Returns and refreshes the cache entry for the given query.
  // Causes undefined behavior if the query is not in the cache.
  std::shared_ptr<hsql::SQLParserResult> get(const std::string& query);

  // Purges all entries from the cache and reinitializes it with the given capacity.
  void clear_and_resize(size_t capacity);

  // Purges all entries from the cache.
  void clear();

  inline size_t size() const { return _cache.size(); }

  inline LRUCache<std::string, std::shared_ptr<hsql::SQLParserResult>>& cache() { return _cache; }

 protected:
  LRUCache<std::string, std::shared_ptr<hsql::SQLParserResult>> _cache;

  std::mutex _mutex;
};

}  // namespace opossum
