#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_repository.hpp"

namespace opossum {

// These symbols provide access to the LLVM bitcode string embedded into the hyriseTest binary during compilation.
// The symbols are defined in an assembly file that is generated in EmbedLLVM.cmake and linked into the binary.
// Please refer to EmbedLLVM.cmake / src/test/CMakeLists.txt for further details of the bitcode generation and
// embedding process.
// The C++ code used to generate the bitcode for this test is located in
// src/test/operators/jit_operator/specialization/modules/jit_repository_test_module.cpp.
extern char jit_repository_test_module;
extern size_t jit_repository_test_module_size;

class JitRepositoryTest : public BaseTest {};

// This test case checks that the JitRepository properly extracts functions and virtual tables from the provided LLVM
// module.
// It uses an example LLVM module with a virtual class hierarchy to test this.
TEST_F(JitRepositoryTest, ProvidesAccessToDefinedFunctions) {
  auto repository = JitRepository(std::string(&jit_repository_test_module, jit_repository_test_module_size));

  // Check that all defined methods in the class hierarchy are present in the bitcode repository.
  // Virtual methods that are not implemented should cause a nullptr.
  // See "src/test/llvm/virtual_methods.cpp" for the class hierarchy.
  ASSERT_EQ(repository.get_function("_ZN4Base3fooEv"), nullptr);
  ASSERT_NE(repository.get_function("_ZN4Base3barEv"), nullptr);
  ASSERT_NE(repository.get_function("_ZN8DerivedA3fooEv"), nullptr);
  ASSERT_EQ(repository.get_function("_ZN8DerivedA3barEv"), nullptr);
  ASSERT_NE(repository.get_function("_ZN8DerivedB3fooEv"), nullptr);
  ASSERT_NE(repository.get_function("_ZN8DerivedB3barEv"), nullptr);
  ASSERT_EQ(repository.get_function("_ZN8DerivedC3fooEv"), nullptr);
  ASSERT_NE(repository.get_function("_ZN8DerivedC3barEv"), nullptr);
  ASSERT_NE(repository.get_function("_ZN8DerivedD3fooEv"), nullptr);
  ASSERT_EQ(repository.get_function("_ZN8DerivedD3barEv"), nullptr);
}

TEST_F(JitRepositoryTest, CorrectlyParsesVTablesAcrossClassHierarchy) {
  auto repository = JitRepository(std::string(&jit_llvm_bundle, jit_llvm_bundle_size));

  auto base_bar = repository.get_function("_ZN4Base3barEv");
  auto derived_a_foo = repository.get_function("_ZN8DerivedA3fooEv");
  auto derived_b_foo = repository.get_function("_ZN8DerivedB3fooEv");
  auto derived_b_bar = repository.get_function("_ZN8DerivedB3barEv");
  auto derived_c_bar = repository.get_function("_ZN8DerivedC3barEv");
  auto derived_d_foo = repository.get_function("_ZN8DerivedD3fooEv");

  // Check that all vtables in the class hierarcy have been parsed correctly
  // and that the correct implementation is returned for each class / index combination.
  // See "src/test/llvm/virtual_methods.cpp" for the class hierarchy.

  ASSERT_EQ(repository.get_vtable_entry("4Base", 0), nullptr);
  ASSERT_EQ(repository.get_vtable_entry("4Base", 1), base_bar);

  ASSERT_EQ(repository.get_vtable_entry("8DerivedA", 0), derived_a_foo);
  ASSERT_EQ(repository.get_vtable_entry("8DerivedA", 1), base_bar);

  ASSERT_EQ(repository.get_vtable_entry("8DerivedB", 0), derived_b_foo);
  ASSERT_EQ(repository.get_vtable_entry("8DerivedB", 1), derived_b_bar);

  ASSERT_EQ(repository.get_vtable_entry("8DerivedC", 0), nullptr);
  ASSERT_EQ(repository.get_vtable_entry("8DerivedC", 1), derived_c_bar);

  ASSERT_EQ(repository.get_vtable_entry("8DerivedD", 0), derived_d_foo);
  ASSERT_EQ(repository.get_vtable_entry("8DerivedD", 1), derived_c_bar);
}

}  // namespace opossum
