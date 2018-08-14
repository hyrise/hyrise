#pragma once

#include "types.hpp"

namespace opossum {

template <typename T>
class Singleton : public Noncopyable {
 protected:
  Singleton() {}
  Singleton& operator=(Singleton&&) = default;

 public:
  // Singleton implementation à la Scott Meyers.
  // The method is declared inline because there can be some trouble with static local variables across translation
  // units (plugins and libhyrise). Functions declared as inline have the following property according to the cpp
  // reference: "Function-local static objects in all function definitions are shared across all translation units
  // (they all refer to the same object defined in one translation unit)".
  inline static T& get() {
    static T instance;
    return instance;
  }

  virtual ~Singleton() {}
};

}  // namespace opossum
