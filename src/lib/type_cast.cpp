#include "type_cast.hpp"

#include "resolve_type.hpp"

namespace opossum {

std::optional<AllTypeVariant> variant_cast_safe(const AllTypeVariant& variant, DataType target_data_type) {
  std::optional<AllTypeVariant> result;

  resolve_data_type(data_type_from_all_type_variant(variant), [&](auto source_data_type_t) {
    using SourceDataType = typename decltype(source_data_type_t)::type;

    resolve_data_type(target_data_type, [&](auto target_data_type_t) {
      using TargetDataType = typename decltype(target_data_type_t)::type;
      const auto source = boost::get<SourceDataType>(variant);
      result = type_cast_safe<TargetDataType>(source);
    });
  });

  return result;
}

}  // namespace opossum
