#include <iostream>

#include "tuning/index_tuner.hpp"

int main() {
  opossum::IndexTuner tuner;

  tuner.execute();

  std::cout << "Hello world!" << std::endl;
  return 0;
}
