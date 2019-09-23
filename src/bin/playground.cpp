#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "synthetic_table_generator.hpp"
#include "types.hpp"

#include "boost/math/distributions/pareto.hpp"
#include "boost/math/distributions/skew_normal.hpp"
#include "boost/math/distributions/uniform.hpp"

using namespace opossum;  // NOLINT

int main() {
  int min = -100000;
  for (int i = 0; i < 100'000'000; ++i) {
    std::random_device random_device;
    auto pseudorandom_engine = std::mt19937{};

    pseudorandom_engine.seed(random_device());
    auto probability_dist = std::uniform_real_distribution{0.0, 1.0};
    auto generate_value_by_distribution_type = std::function<int(void)>{};

    const auto skew_dist = boost::math::skew_normal_distribution<double>{1000.0, 1.0, 1.0};
    generate_value_by_distribution_type = [skew_dist, &probability_dist, &pseudorandom_engine]() {
      const auto probability = probability_dist(pseudorandom_engine);
      return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
    };
    const auto value = generate_value_by_distribution_type();
    min = std::min(min, value);
    if (value < 0) {
      std::cout << i << " - " << value << std::endl;
    }
  }
  std::cout << "Done ... " << min << std::endl;
}
