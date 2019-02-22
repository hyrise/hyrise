#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "selectivity.hpp"

namespace opossum {

class AbstractStatisticsObject;
enum class PredicateCondition;

/**
 * Base class for VerticalStatisticsSlice<T>
 */
class BaseVerticalStatisticsSlice {
 public:
  explicit BaseVerticalStatisticsSlice(const DataType data_type);
  virtual ~BaseVerticalStatisticsSlice() = default;

  virtual void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) = 0;

  virtual std::shared_ptr<BaseVerticalStatisticsSlice> scaled(const Selectivity selectivity) const = 0;

  virtual std::shared_ptr<BaseVerticalStatisticsSlice> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  virtual bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  const DataType data_type;
};

}  // namespace opossum
