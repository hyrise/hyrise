#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "lru_cache.hpp"

#include "SQLParserResult.h"

namespace opossum {

class SQLParseTreeCache {
 public:
  SQLParseTreeCache(size_t capacity);

  virtual ~SQLParseTreeCache();

  // Takes ownership of the result.
  void set(const std::string& query, std::shared_ptr<hsql::SQLParserResult> result);

  bool has(const std::string& query) const;

  std::shared_ptr<hsql::SQLParserResult> get(const std::string& query);

  void reset(size_t capacity);

 protected:
  LRUCache<std::string, std::shared_ptr<hsql::SQLParserResult>> _cache;

  std::mutex _mutex;
};

}  // namespace opossum
