#include <iostream>
#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <vector>

#include "types.hpp"

using namespace opossum;  // NOLINT

struct root {};

int main() {
  std::cout << "Hello world!!" << std::endl;

  auto pool = pmem::obj::pool<root>::create("poolfile", "foo", 10'000'000ul);

  for(auto i = 0; i < 1000; ++i) {
	  std::vector<int, pmem::obj::allocator<int>> test;

	  pmem::obj::transaction::run(pool, [&] {
		  std::cout << i << " " << pmem::obj::allocator<char>{}.allocate(1'000'000) << std::endl;
		  // test.push_back(1);
	  });

	  // std::cout << test[0] << std::endl;

	  // pmem::obj::transaction::run(pool, [&] {
		 //  test.clear();
		 //  test.shrink_to_fit();
	  // });
	 }

  return 0;
}
