#pragma once

#include "types.hpp"

namespace hyrise {

// Singleton implementation à la Scott Meyers.
// The method is declared inline because there can be some trouble with static local variables across translation
// units (plugins and libhyrise). Static objects of functions declared as inline have the following property
// according to the cpp reference: "Function-local static objects in all function definitions are shared across all
// translation units (they all refer to the same object defined in one translation unit)".
template <typename T>
class Singleton : public Noncopyable {
 public:
  static T& get() {
    static T instance;
    return instance;
  }

  ~Singleton() override = default;
  // NOLINTNEXTLINE(bugprone-crtp-constructor-accessibility)
  Singleton(const Singleton&) = delete;
  Singleton& operator=(const Singleton&) = delete;

 protected:
  // If you need to overwrite the constructor make sure to friend this Singleton class. Otherwise it cannot call
  // the protected constructor of a derived class.
 private:
  Singleton() = default;

  Singleton& operator=(Singleton&&) = default;
  Singleton(Singleton&&) = default;
  friend T;
};

}  // namespace hyrise
