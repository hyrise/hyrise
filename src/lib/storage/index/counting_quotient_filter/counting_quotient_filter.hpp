#pragma once

#include <cqf.hpp>
#include "types.hpp"
#include "storage/index/base_filter.hpp"

#include <cstdint>
#include <vector>
#include <string>
#include <cstdlib>
#include <ctime>


namespace opossum {

/**
Following the idea and implementation of Pandey, Johnson and Patro:
Paper: A General-Purpose Counting Filter: Making Every Bit Count
Repository: https://github.com/splatlab/cqf
**/
template <typename ElementType>
class CountingQuotientFilter : public BaseFilter {
 public:
  CountingQuotientFilter(uint8_t quotient_bits, uint8_t remainder_bits);
  virtual ~CountingQuotientFilter();// = default;
  void insert(ElementType value, uint64_t count);
  void insert(ElementType value);
  void populate(std::shared_ptr<const BaseColumn> column) override;
  uint64_t count(ElementType value) const;
  uint64_t count_all_type(AllTypeVariant value) const final;
  uint64_t memory_consumption() const final;
  double load_factor() const final;
  bool is_full() const final;

 private:
  std::optional<gqf::quotient_filter> _quotient_filter;
  uint64_t _quotient_bits;
  uint64_t _remainder_bits;
  uint64_t _number_of_slots;
  uint64_t _hash_bits;
  uint64_t _hash(ElementType value) const;
  const uint32_t _seed = std::rand();

};

} // namespace opossum
