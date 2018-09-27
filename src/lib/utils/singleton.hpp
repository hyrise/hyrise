#pragma once

#include "types.hpp"

namespace opossum {

// Singleton implementation à la Scott Meyers.
// The method is declared inline because there can be some trouble with static local variables across translation
// units (plugins and libhyrise). Static objects of functions declared as inline have the following property
// according to the cpp reference: "Function-local static objects in all function definitions are shared across all
// translation units (they all refer to the same object defined in one translation unit)".
template <typename T>
class Singleton : public Noncopyable {
 public:
  inline static T& get() {
    static T instance;
    return instance;
  }

  virtual ~Singleton() {}

 protected:
  // If you need to overwrite the constructor make sure to friend this Singleton class. Otherwise it cannot call
  // the protected constructor of a derived class.
  Singleton() {}
  Singleton& operator=(Singleton&&) = default;
};

}  // namespace opossum
