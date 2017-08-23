#pragma once

#include <boost/hana/for_each.hpp>

#include <memory>
#include <string>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * Resolves a column type by passing a hana::type object on to a generic lambda
 *
 * @param type is a string representation of any of the supported column types
 * @param func is generic lambda or similar
 */
template <typename Functor>
void resolve_type(const std::string &type, const Functor &func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value into a value
      func(+hana::second(x));
    }
  });
}

/**
 * Resolves a column type by passing a hana::type object and the resolved column on to a generic lambda
 *
 * Example:
 *   resolve_column_type(column_type, base_column, [&] (auto type, auto &typed_column) {
 *     using Type = typename decltype(type)::type;
 *     using ColumnType = typename std::decay<decltype(typed_column)>::type;
 *
 *     constexpr auto is_reference_column = (std::is_same<LeftColumnType, ReferenceColumn>{});
 *     constexpr auto is_string_column = (std::is_same<Type, std::string>{});
 *
 *     method_expecting_template_type<Type>();
 *   });
 *
 * @param type is a string representation of any of the supported column types
 * @param func is generic lambda or similar accepting two parameters: a hana::type object and
 *   a reference to a specialized column (value, dictionary, reference)
 */
template <typename Functor>
void resolve_column_type(const std::string &type, BaseColumn &column, const Functor &func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value
      // into a value so that we can access ::type
      using Type = typename decltype(+hana::second(x))::type;

      struct Context : public ColumnVisitableContext {
        explicit Context(const Functor &f) : func{f} {}
        const Functor &func;
      };

      struct Visitable : public ColumnVisitable {
        void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> c) override {
          const auto context = std::static_pointer_cast<Context>(c);
          auto &column = static_cast<ValueColumn<Type> &>(base_column);

          context->func(hana::type_c<Type>, column);
        }

        void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> c) override {
          const auto context = std::static_pointer_cast<Context>(c);
          auto &column = static_cast<DictionaryColumn<Type> &>(base_column);

          context->func(hana::type_c<Type>, column);
        }

        void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> c) override {
          const auto context = std::static_pointer_cast<Context>(c);

          context->func(hana::type_c<Type>, column);
        }
      };

      auto visitable = Visitable{};
      auto context = std::make_shared<Context>(func);
      column.visit(visitable, context);
    }
  });
}

}  // namespace opossum
