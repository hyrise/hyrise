class Base {
  virtual void foo() = 0;
  virtual void bar() {}
};

class DerivedA : public Base {
  void foo() {}
};

class DerivedB : public Base {
  void foo() {}
  void bar() {}
};

class DerivedC : public Base {
  void bar() {}
};


class DerivedD : public DerivedC {
  void foo() {}
};

// Prevent LLVM from optimizing away the entire class hierarchy
int main() {
  DerivedA a;
  DerivedB c;
  DerivedD d;
}
