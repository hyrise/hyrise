#pragma once

#include <string>
#include <vector>

#include "cqf16.hpp"
#include "cqf2.hpp"
#include "cqf32.hpp"
#include "cqf4.hpp"
#include "cqf8.hpp"
#include "types.hpp"

#include "abstract_filter.hpp"
#include "storage/base_segment.hpp"

namespace opossum {

/* Counting Quotient Filters allow you to keep track of which values are present in a segment and how often. Filters
work approximately. If a membership query yields a positive result, the value is probably present but there is
a chance of a false positive. If the query delivers a negative result, the item is guaranteed to not be contained.
In the same way, items can be over counted but not under counted.
CQF can be configured with quotient size, which determines the number of slots, and the remainder size, which
corresponds to the slot size. At this time, the remainder size must be 2, 4, 8, 16 or 32.

"When you configure the CQF the number of slots in the CQF must be at least the number of the distinct elements in your
input dataset plus the sum of logs of all the counts divided by the remainder size. You must have some estimate of the
number distinct elements in your dataset to configure the CQF correctly. For example, if in a dataset there are `M`
integers and `N` distinct integers. And let's assume each integer appears M/N times. Then the number of slots `S` you
would need would be `S = N*(1 + log(M/N)/r)`. Since the number of slots can only be a power of two, we choose the
smallest number greater than `S` that is a power of 2 as the number of slots." - Prashant Pandey
*/

template <typename ElementType>
class CountingQuotientFilter : public AbstractFilter, public Noncopyable {
 public:
  CountingQuotientFilter(const size_t quotient_size, const size_t remainder_size);
  ~CountingQuotientFilter() override;

  void insert(ElementType value, size_t count = 1);
  void populate(const std::shared_ptr<const BaseSegment>& segment);

  size_t count(const ElementType& value) const;
  size_t count(const AllTypeVariant& value) const;

  size_t memory_consumption() const;

  float load_factor() const;

  bool is_full() const;

  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  // Can't copy CountingQuotientFilter
  CountingQuotientFilter(CountingQuotientFilter&) = delete;
  CountingQuotientFilter(CountingQuotientFilter&&) = delete;
  CountingQuotientFilter operator=(CountingQuotientFilter&) = delete;
  CountingQuotientFilter operator=(CountingQuotientFilter&&) = delete;

 private:
  size_t _hash(const ElementType& value) const;

  boost::variant<gqf2::QF, gqf4::QF, gqf8::QF, gqf16::QF, gqf32::QF> _quotient_filter;
  const size_t _hash_bits;
};

}  // namespace opossum
