#pragma once

#include <iostream>
#include <memory>
#include <optional>

#include "min_max_filter.hpp"
#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class DipsMinMaxFilter : public MinMaxFilter<T> {
 public:
  explicit DipsMinMaxFilter(T init_min, T init_max, CommitID init_commitID);

  const CommitID commitID;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const DipsMinMaxFilter<T>& filter) {
  stream << "{" << filter.min << " " << filter.max << "} t: " << filter.commitID;
  return stream;
}

}  // namespace opossum
