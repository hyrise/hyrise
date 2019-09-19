#include <iostream>

#include "types.hpp"
#include "synthetic_table_generator.hpp"

using namespace opossum;  // NOLINT

int main() {
	for (int i = 0; i < 4'000'000; ++i) {
		if (SyntheticTableGenerator::generate_value<pmr_string>(i) == "      EPIC") {
			std::cout << i << std::endl;
		}
	}
}
