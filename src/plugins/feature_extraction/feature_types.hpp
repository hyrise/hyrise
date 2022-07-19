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

using Feature = double;

using FeatureVector = std::vector<Feature>;

void feature_vector_to_stream(const FeatureVector& feature_vector, std::ostream& stream);

std::string feature_vector_to_string(const FeatureVector& feature_vector);

std::ostream& operator<<(std::ostream& stream, const FeatureVector& feature_vector);

}  // namespace opossum
