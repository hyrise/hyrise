#include "lossless_cast.hpp"

#include "resolve_type.hpp"

namespace opossum {

std::optional<AllTypeVariant> lossless_variant_cast(const AllTypeVariant& variant, DataType target_data_type) {
  const auto source_data_type = data_type_from_all_type_variant(variant);

  // Lossless casting from NULL to NULL is always NULL. (Cannot be handled below as resolve_data_type()
  // doesn't resolve NULL)
  if (source_data_type == DataType::Null && target_data_type == DataType::Null) {
    return NullValue{};
  }

  // Safe casting between NULL and non-NULL type is not possible. (Cannot be handled below as resolve_data_type()
  // doesn't resolve NULL)
  if ((source_data_type == DataType::Null) != (target_data_type == DataType::Null)) {
    return std::nullopt;
  }

  std::optional<AllTypeVariant> result;

  // clang-format off
  boost::apply_visitor([&](const auto& source) {
    resolve_data_type(target_data_type, [&](auto target_data_type_t) {
      using TargetDataType = typename decltype(target_data_type_t)::type;
      result = lossless_cast<TargetDataType>(source);
    });
  }, variant);
  // clang-format on

  return result;
}

}  // namespace opossum
