#include <iostream>

#include "types.hpp"
#include "expression/expression_functional.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;

std::shared_ptr<AbstractExpression> get_ex() {
  //return nullptr;
  return value_(int32_t{1});
}

int main() {
  std::cout << "Hello world!!" << std::endl;

  std::shared_ptr<AbstractExpression> value_1 = value_(int32_t{1});
  auto value_1_1 = get_ex(); //value_(int32_t{1});

  if (value_1_1 == value_1) {
    std::cout << "1 == 1_1" << std::endl;
  } else {
    std::cout << "1 != 1_1" << std::endl;
  }

    if (*value_1_1 == *value_1) {
    std::cout << "*1 == *1_1" << std::endl;
  } else {
    std::cout << "*1 != *1_1" << std::endl;
  }

  auto vec = std::vector<std::shared_ptr<AbstractExpression>>{};

  if (vec.front() == value_1) {
    std::cout << "front == 1" << std::endl;
  } else {
    std::cout << "front != 1" << std::endl;
  }

  if (*vec.front() == *value_1) {
    std::cout << "*front == *1" << std::endl;
  } else {
    std::cout << "*front != *1" << std::endl;
  }

  if (vec.front() == nullptr) {
    std::cout << "front == nullptr" << std::endl;
  } else {
    std::cout << "front != nullptr" << std::endl;
  }

    if (nullptr == value_1) {
    std::cout << "1 == nullptr" << std::endl;
  } else {
    std::cout << "1 != nullptr" << std::endl;
  }


  return 0;
}
