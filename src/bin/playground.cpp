#include <iostream>

#include "types.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  auto reader = SharedMemoryReader<128, 16>(SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  reader.read_data();
  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << std::endl;
  return 0;
}
