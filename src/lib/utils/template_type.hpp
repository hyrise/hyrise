#pragma once

#include <boost/hana/type.hpp>

namespace opossum {

/**
 * Wraps a class template so that
 * it can be stored in a hana::type
 */
template <template <typename...> typename TemplateT>
struct TemplateType {
  template <typename T>
  using _template = TemplateT<T>;
};

/**
 * Variable template to conveniently create
 * hana::type objects from class templates.
 */
template <template <typename...> typename TemplateT>
constexpr auto template_c = hana::type_c<TemplateType<TemplateT>>;

}  // namespace opossum
