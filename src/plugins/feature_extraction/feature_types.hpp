#pragma once

#include "types.hpp"

namespace opossum {

struct Query {
  Query(const std::string& init_hash, const std::string& init_query, const size_t init_frequency)
    : hash{init_hash}, query{init_query}, frequency{init_frequency} {}
  std::string hash;
  std::string query;
  size_t frequency;
};

using FeatureVector = std::vector<double>;

}  // namespace opossum
