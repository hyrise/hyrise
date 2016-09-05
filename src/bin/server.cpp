#include <iostream>

int main() {
  std::cout << "Built as: " << (IS_DEBUG ? "debug" : "release") << std::endl;
  std::cout << "This is the server. As of now, it does nothing." << std::endl;
  return 0;
}
