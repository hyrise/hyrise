#include <iostream>

struct Base {
  virtual void foo() = 0;
  virtual void bar() { std::cout << "Base::bar()" << std::endl; }
};

struct DerivedA : public Base {
  void foo() { std::cout << "DerivedA::foo()" << std::endl; }
};

struct DerivedB : public Base {
  void foo() { std::cout << "DerivedB::foo()" << std::endl; }
  void bar() { std::cout << "DerivedB::bar()" << std::endl; }
};

struct DerivedC : public Base {
  void bar() { std::cout << "DerivedC::bar()" << std::endl; }
};

struct DerivedD : public DerivedC {
  void foo() { std::cout << "DerivedD::foo()" << std::endl; }
};

// Prevent LLVM from optimizing away the entire class hierarchy
int main() {
  DerivedA a;
  DerivedB c;
  DerivedD d;
  a.foo();
  a.bar();
  c.foo();
  c.bar();
  d.foo();
  d.bar();
}
