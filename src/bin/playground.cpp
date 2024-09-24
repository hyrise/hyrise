#include <iostream>
#include <unordered_set>

#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/operator_task.hpp"
#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

struct RefWapperAddressHash {
  std::size_t operator()(AbstractTask const& obj) const {
    std::hash<AbstractTask const*> theHash{};
    return theHash(&obj);
  }
};

int main() {
  const auto world = pmr_string{"world"};
  std::cout << "Hello " << world << "!\n";

  auto test_1 = std::make_shared<JobTask>([] {});
  // auto test_2 = std::make_shared<OperatorTask>([] {});

  auto abstr_ptr = std::shared_ptr<AbstractTask>{};

  abstr_ptr = test_1;
  // abstr_ptr = test_2;

  [[maybe_unused]] auto ref_1 = std::reference_wrapper<AbstractTask>(std::ref(*test_1));
  // auto ref_2 = std::reference_wrapper<AbstractTask>(&*test_2);

  [[maybe_unused]] std::unordered_set<std::reference_wrapper<AbstractTask>, RefWapperAddressHash> test{};
  // test.emplace(ref_1);

  return 0;
}
