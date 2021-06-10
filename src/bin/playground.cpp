#include <iostream>

#include <fsst.h>
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::vector<std::string> values{"Moritz", "Chris", "Christopher", "Mo", "Peter", "Petrus"};
  std::vector<unsigned long> row_lens, compressedRowLens;
  std::vector<unsigned char*> row_ptrs, compressedRowPtrs;

  row_lens.reserve(values.size());
  row_ptrs.reserve(values.size());
  compressedRowLens.resize(values.size());
  compressedRowPtrs.resize(values.size() + 1);

  unsigned totalLen = 0;

  for(std::string& value: values){
    totalLen += value.size();
    row_lens.push_back(value.size());
    row_ptrs.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));
  }

  std::vector<unsigned char> compressionBuffer, fullBuffer;

  compressionBuffer.resize(16 + 2 * totalLen);

  fsst_encoder_t* encoder = fsst_create(values.size(), row_lens.data(), row_ptrs.data(), 1);
  fsst_compress(encoder, values.size(), row_lens.data(), row_ptrs.data(), compressionBuffer.size(), compressionBuffer.data(), compressedRowLens.data(), compressedRowPtrs.data());

  for(size_t i = 0; i < compressedRowPtrs.size(); i ++) {
    for(size_t j = 0; j < compressedRowLens[i]; j++){

      printf("%d ", compressedRowPtrs[i][j]);
    }
    printf("\n");
  }
//  std::cout << "blaaa";
  return 0;
}
