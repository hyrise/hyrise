#pragma once

#include "types.hpp"

namespace opossum {

template <typename T>
class Singleton : public Noncopyable {
 protected:
  Singleton() {};

  Singleton& operator=(Singleton&&) = default;

 public:

  virtual ~Singleton() {};

  // Singleton
  inline static T& get() {
    static T instance;
    // Singleton<T>& t = instance;
    // t.init();
    return instance;
  }
};

}  // namespace opossum
