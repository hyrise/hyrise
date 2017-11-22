#pragma once

#include <boost/hana/at_key.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/tuple.hpp>

#include "base_ns_vector.hpp"
#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

namespace detail {

constexpr auto ns_type_for_uint_type =
    hana::make_map(hana::make_pair(hana::type_c<uint32_t>, NsType::FixedSize32ByteAligned),
                   hana::make_pair(hana::type_c<uint16_t>, NsType::FixedSize16ByteAligned),
                   hana::make_pair(hana::type_c<uint8_t>, NsType::FixedSize8ByteAligned));

}  // namespace detail

template <typename UnsignedIntType>
class FixedSizeByteAlignedVector : public BaseNsVector {
 public:
  FixedSizeByteAlignedVector(pmr_vector<UnsignedIntType> data) : _data{std::move(data)} {}
  ~FixedSizeByteAlignedVector() final = default;

  size_t size() const final { return _data.size(); }
  size_t data_size() const final { return sizeof(UnsignedIntType) * _data.size(); }
  NsType type() const final { return detail::ns_type_for_uint_type[hana::type_c<UnsignedIntType>]; }

  const pmr_vector<UnsignedIntType>& data() const;

 private:
  const pmr_vector<UnsignedIntType> _data;
};

}  // namespace opossum
