#pragma once

#include <vector>
#include <string>

#include "cqf2.hpp"
#include "cqf4.hpp"
#include "cqf8.hpp"
#include "cqf16.hpp"
#include "cqf32.hpp"
#include "types.hpp"

#include "storage/base_segment.hpp"
#include "abstract_filter.hpp"




namespace opossum {

/* Counting Quotient Filters allow you to keep track of which values are present in a segment and how often. Filters
work approximately. If a membership query yields a positive result, the value is probably present but there is
a chance of a false positive. If the query delivers a negative result, the item is guaranteed to not be contained.
In the same way, items can be over counted but not under counted.
CQF can be configured with quotient size, which determines the number of slots, and the remainder size, which
corresponds to the slot size. At this time, the remainder size must be 2, 4, 8, 16 or 32. */

template <typename ElementType>
class CountingQuotientFilter : AbstractFilter {
 public:
  CountingQuotientFilter(uint8_t quotient_bits, uint8_t remainder_size);
  virtual ~CountingQuotientFilter();
  void insert(ElementType value, uint64_t count);
  void insert(ElementType value);
  void populate(std::shared_ptr<const BaseSegment> segment);
  uint64_t count(ElementType value) const;
  uint64_t count_all_type(AllTypeVariant value) const;
  uint64_t memory_consumption() const;
  double load_factor() const;
  bool is_full() const;

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override;

 private:
  boost::variant<gqf2::QF, gqf4::QF, gqf8::QF, gqf16::QF, gqf32::QF> _quotient_filter;
  uint8_t _remainder_size;
  uint64_t _number_of_slots;
  uint64_t _hash_bits;
  uint64_t _hash(ElementType value) const;
  const uint32_t _seed = std::rand();
};

}  // namespace opossum
