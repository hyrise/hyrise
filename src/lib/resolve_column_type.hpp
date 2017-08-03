#pragma once

#include <boost/hana/for_each.hpp>

#include <memory>
#include <string>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/value_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

namespace hana = boost::hana;

template <typename Functor>
void resolve_type(const std::string & type, const Functor & func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value into a value
      func(+hana::second(x));
    }
  });
}

template <typename Functor>
void resolve_column_type(const std::string & type, BaseColumn & column, const Functor & func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value
      // into a value so that we can access ::type
      using Type = typename decltype(+hana::second(x))::type;

      struct Context : public ColumnVisitableContext {
        Context(const Functor & f) : func{f} {}
        const Functor & func;
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
          Fail("resolve_column_type does not handle reference columns.");
        }
      };

      auto visitable = Visitable{};
      auto context = std::make_shared<Context>(func);
      column.visit(visitable, context);
    }
  });
}

}  // namespace opossum
