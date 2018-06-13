#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <llvm/IR/Value.h>

#include "utils/assert.hpp"

namespace opossum {

/* The JitRuntimePointer class hierarchy represents concrete memory locations of LLVM bitcode pointer values.
 * A primer on LLVM pointer values:
 * LLVM provides a number of instructions for performing operations on pointer values.
 * LLVM, however, does not have any concept of concrete, materialized pointer values. Pointers represent the memory
 * location of a value in an abstract way, but LLVM has no way of knowing where a value will be located in memory at
 * compile-time. Nonetheless, several operations can be performed on these abstract pointer values.
 * - Bitcast instructions change the data type of a pointer, but does not change the value of a pointer.
 * - Load instructions load a piece of memory into a variable, similar to dereferencing pointers in C++.
 * - GetElementPtr instructions apply an offset to a pointer value (e.g., to access a member field in a
 *   structure type)
 * These instructions can be combined to navigate arbitrarily nested and linked data structures.
 * We represent a set of LLVM pointer instructions that access some nested value by a chain of JitRuntimePointer
 * subclasses.
 * Each type of LLVM instruction is represented by a specific JitRuntimePointer subclass that can simulate the
 * equivalent operation on a "real" pointer.
 * All of these chains end with a JitConstantRuntimePointer that contains a constant pointer value. This value is not
 * fetched from the LLVM bitcode, but must be supplied by the caller. It serves as the source for runtime information.
 * Starting from this value, that chain of JitRuntimePointer instances can simulate the pointer operations in the LLVM
 * bitcode.
 * The JitRuntimePointer class hierarchy provides several helper functions to navigate these nested memory operations
 * (e.g., to get the total offset of consecutive GetElementPtr instructions).
 * The JitRuntimePointer base class represents pointer values that cannot be resolved to a physical address and are
 * thus invalid.
 *
 * Example:
 * struct TreeNode {
 *   int32_t value;
 *   TreeNode* left_child;
 *   TreeNode* right_child;
 * }
 *
 * Accessing the value of some internal node like so ...
 *
 * root->left_child->right_child->value;
 *
 * ... roughly translates to the following pointer instructions in LLVM:
 * %1 = getelementptr %root, 4  // add an offset to the root pointer to locate the "left_child" field within the object
 * %2 = load %1                 // dereference this pointer to get a pointer to the actual left_child
 * %3 = getelementptr %2, 12    // add an offset to the left_child pointer to locate the "right_child" field
 * %4 = load %3                 // dereference this pointer to get a pointer to the actual right_child
 * %5 = bitcast %4 to int32_t*  // no need to apply an offset, since the offset to the value is 0,
 *                              // but the pointer type needs to change
 * %6 = load %5                 // dereference the casted pointer to load the actual value
 *
 * If the memory location of the root pointer is known, the physical address of the desired value can be computed by the
 * JitRuntimePointer class hierarchy.
 */
class JitRuntimePointer {
 public:
  virtual ~JitRuntimePointer() {}

  // Returns whether the JitRuntimePointer can be resolved to a valid physical address.
  virtual bool is_valid() const { return false; }
};

class JitKnownRuntimePointer : public JitRuntimePointer {
 public:
  // Checks whether the address pointed to is valid (i.e., can be dereferenced).
  // This solution is based on https://stackoverflow.com/questions/4611776/isbadreadptr-analogue-on-unix
  bool is_valid() const override {
    const auto ptr = reinterpret_cast<void*>(address());
    auto fd = open("/dev/random", O_WRONLY);
    bool result = (write(fd, ptr, 8) == 8);
    close(fd);
    return result;
  }

  // Returns the runtime address of this pointer
  virtual uint64_t address() const = 0;

  // Returns the total offset applied to a pointer across multiple GetElementPtr instructions
  virtual uint64_t total_offset() const = 0;

  // Returns the "base" pointer. This is a pointer without any applied offset (i.e., the pointer that points to the
  // beginning of an object instance).
  // In the above example, %root is the "base" pointer of %1 and %2 is the "base" pointer of %3.
  virtual const JitKnownRuntimePointer& base() const { Fail("Cannot get base pointer."); }

  // Returns the pointer one level "up" in the pointer hierarchy. This is the opposite of dereferencing a pointer.
  // In the above example, %1 is the "up" pointer of %2 and %3 is the "up" pointer of %4.
  virtual const JitKnownRuntimePointer& up() const { Fail("Cannot move pointer up."); }
};

/*
 * A runtime pointer that is initialized with a known physical address. This type of pointer constitutes the endpoint
 * of any JitRuntimePointer chain that represents a valid location.
 */
class JitConstantRuntimePointer : public JitKnownRuntimePointer {
 public:
  explicit JitConstantRuntimePointer(const uint64_t address) : _address{address} {}
  template <typename T>
  explicit JitConstantRuntimePointer(const T* ptr) : _address{reinterpret_cast<uint64_t>(ptr)} {}
  template <typename T>
  explicit JitConstantRuntimePointer(const std::shared_ptr<T>& ptr) : _address{reinterpret_cast<uint64_t>(ptr.get())} {}

  uint64_t address() const final { return _address; }

  uint64_t total_offset() const final { return 0l; }

  const JitKnownRuntimePointer& base() const final { return *this; }

 private:
  const uint64_t _address;
};

/*
 * Represents a GetElementPtr instruction that applies an offset to a given pointer
 */
class JitOffsetRuntimePointer : public JitKnownRuntimePointer {
 public:
  JitOffsetRuntimePointer(const std::shared_ptr<const JitKnownRuntimePointer>& base, const uint64_t offset)
      : _base{base}, _offset{offset} {}

  bool is_valid() const final { return _base->is_valid() && JitKnownRuntimePointer::is_valid(); }

  uint64_t address() const final { return _base->address() + _offset; }

  uint64_t total_offset() const final { return _base->total_offset() + _offset; }

  const JitKnownRuntimePointer& base() const final { return _base->base(); }

  const JitKnownRuntimePointer& up() const final { return _base->up(); }

 private:
  // the pointer that an offset is applied to
  const std::shared_ptr<const JitKnownRuntimePointer> _base;
  // the offset that is being applied
  const uint64_t _offset;
};

/*
 * Represents a Load instruction that dereferences a given pointer
 */
class JitDereferencedRuntimePointer : public JitKnownRuntimePointer {
 public:
  explicit JitDereferencedRuntimePointer(const std::shared_ptr<const JitKnownRuntimePointer>& base) : _base{base} {}

  bool is_valid() const final { return _base->is_valid() && JitKnownRuntimePointer::is_valid(); }

  uint64_t address() const final { return *reinterpret_cast<uint64_t*>(_base->address()); }

  uint64_t total_offset() const final { return 0l; }

  const JitKnownRuntimePointer& base() const final { return *this; }

  const JitKnownRuntimePointer& up() const final { return *_base; }

 private:
  // the pointer that is being dereferenced
  const std::shared_ptr<const JitKnownRuntimePointer> _base;
};

}  // namespace opossum
