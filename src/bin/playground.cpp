#include <iostream>

#include "import_export/data_generation/shared_memory_reader.hpp"
#include "import_export/data_generation/pdgf_process.hpp"

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  auto reader = SharedMemoryReader<128, 16>(Chunk::DEFAULT_SIZE, SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  auto pdgf = PdgfProcess(PDGF_DIRECTORY_ROOT);
  pdgf.run();

  while (reader.has_next_table()) {
    reader.read_next_table();
  }
  pdgf.wait();

  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
  return 0;
}
