#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

class AbstractColumnStatistics {
 protected:
  virtual std::ostream &to_stream(std::ostream &os) = 0;

 public:
  friend std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj) { return obj.to_stream(os); }
  virtual std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const std::string &op, const AllTypeVariant value, const optional<AllTypeVariant> value2) = 0;
  virtual std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const std::string& op, const std::shared_ptr<AbstractColumnStatistics> value_column_statistics, const optional<AllTypeVariant> value2) = 0;
  virtual ~AbstractColumnStatistics() = default;

  AbstractColumnStatistics() = default;
};

}  // namespace opossum
