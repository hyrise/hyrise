#pragma once

#include "types.hpp"

namespace opossum {

template <typename T>
class Singleton : public Noncopyable {
 protected:
  Singleton() {}
  Singleton& operator=(Singleton&&) = default;

 public:
  // Singleton implementation à la Scott Meyers
  inline static T& get() {
    static T instance;
    return instance;
  }

  virtual ~Singleton() {}
};

}  // namespace opossum
