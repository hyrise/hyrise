#include "iostream"
#include "types.hpp"
#include "dummy_mvcc_delete.hpp"

using namespace opossum;  // NOLINT

int main() {
  auto& dmd = DummyMvccDelete::get();
  dmd.start();
  return 0;
}
