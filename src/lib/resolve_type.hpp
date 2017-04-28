#pragma once

#include <boost/hana/for_each.hpp>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

template <class base, template <typename...> class impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<base> make_unique_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  std::unique_ptr<base> ret = nullptr;
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value
      // into a value so that we can access ::type
      using column_type = typename decltype(+hana::second(x))::type;
      ret = std::make_unique<impl<column_type, TemplateArgs...>>(std::forward<ConstructorArgs>(args)...);
      return;
    }
  });
  if (IS_DEBUG && !ret) throw std::runtime_error("unknown type " + type);
  return ret;
}

/**
 * We need to pass parameter packs explicitly for GCC due to the following bug:
 * http://stackoverflow.com/questions/41769851/gcc-causes-segfault-for-lambda-captured-parameter-pack
 */
template <class base, template <typename...> class impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<base> make_unique_by_column_types(const std::string &type1, const std::string &type2,
                                                  ConstructorArgs &&... args) {
  std::unique_ptr<base> ret = nullptr;
  hana::for_each(column_types, [&ret, &type1, &type2, &args...](auto x) {
    if (std::string(hana::first(x)) == type1) {
      hana::for_each(column_types, [&ret, &type2, &args...](auto y) {
        if (std::string(hana::first(y)) == type2) {
          using column_type1 = typename decltype(+hana::second(x))::type;
          using column_type2 = typename decltype(+hana::second(y))::type;
          ret = std::make_unique<impl<column_type1, column_type2, TemplateArgs...>>(
              std::forward<ConstructorArgs>(args)...);
          return;
        }
      });
      return;
    }
  });
  if (IS_DEBUG && !ret) throw std::runtime_error("unknown type " + type1 + " or " + type2);
  return ret;
}

template <class base, template <typename...> class impl, class... TemplateArgs, class... ConstructorArgs>
std::shared_ptr<base> make_shared_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  return make_unique_by_column_type<base, impl, TemplateArgs...>(type, std::forward<ConstructorArgs>(args)...);
}

template <typename Functor, typename... Args>
void call_functor_by_column_type(const std::string &type, Args &&... args) {
  // In some cases, we want to call a function depending on the type of a column. Here we do not want to create an
  // object that would require an untemplated base class. Instead, we can create a functor class and have a templated
  // "run" method. E.g.:

  // class MyFunctor {
  // public:
  //   template <typename T>
  //   static void run(int some_arg, int other_arg) {
  //     std::cout << "Column type is " << typeid(T).name() << ", args are " << some_arg << " and " << other_arg <<
  //     std::endl;
  //   }
  // };
  //
  // std::string column_type = "int";
  // call_functor_by_column_type<MyFunctor>(column_type, 2, 3);

  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      using column_type = typename decltype(+hana::second(x))::type;
      Functor::template run<column_type>(std::forward<Args>(args)...);
    }
  });
}

}  // namespace opossum
