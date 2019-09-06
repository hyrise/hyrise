#include <iostream>

#include <boost/lexical_cast.hpp>
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::vector<char> v = {'a', '2'};
  std::cout << "vector: " << v.size() << std::endl;
  float asd = 1.09234242342f;
  float asd1 = 1.099999999999f;
  double asd2 = 1.099999999999;
  double asd3 = 99999999999;
  std::cout << "boost:" << boost::lexical_cast<std::string>(asd) << ", std:" << std::to_string(asd) << std::endl;
  std::cout << "boost:" << boost::lexical_cast<std::string>(asd1) << ", std:" << std::to_string(asd1) << std::endl;
  std::cout << "boost:" << boost::lexical_cast<std::string>(asd2) << ", std:" << std::to_string(asd2) << std::endl;
  std::cout << "boost:" << boost::lexical_cast<std::string>(asd3) << ", std:" << std::to_string(asd3) << std::endl;
  return 0;
}
