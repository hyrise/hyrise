#include <iostream>

#include <fsst.h>
#include <fstream>
#include <iostream>
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::vector<std::string> values{
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab",
      "Moritz", "ChrisChr", "Christopher", "Mo", "Peter", "Petrus", "ababababababababababab"};

  std::vector<unsigned long> row_lens, compressedRowLens;
  std::vector<unsigned char*> row_ptrs, compressedRowPtrs;

  row_lens.reserve(values.size());
  row_ptrs.reserve(values.size());
  compressedRowLens.resize(values.size());
  compressedRowPtrs.resize(values.size());

  unsigned totalLen = 0;

  for (std::string& value : values) {
    totalLen += value.size();
    row_lens.push_back(value.size());
//    row_ptrs.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));
      row_ptrs.push_back(reinterpret_cast<unsigned char* const>(value.data()));
  }

  std::vector<unsigned char> compressionBuffer, fullBuffer;

  compressionBuffer.resize(16 + 2 * totalLen);

  // COMPRESSION
  fsst_encoder_t* encoder = fsst_create(values.size(), row_lens.data(), row_ptrs.data(), 0);

  unsigned char buffer[sizeof(fsst_decoder_t)];
  fsst_export(encoder, buffer);

  fsst_compress(encoder, values.size(), row_lens.data(), row_ptrs.data(), compressionBuffer.size(),
                compressionBuffer.data(), compressedRowLens.data(), compressedRowPtrs.data());
  //  fsst_destroy(encoder);

  for (size_t i = 0; i < compressedRowPtrs.size(); i++) {
    for (size_t j = 0; j < compressedRowLens[i]; j++) {
      printf("%d ", compressedRowPtrs[i][j]);
    }
    printf("\n");
  }

  // DECOMPRESSION
  fsst_decoder_t decoder = fsst_decoder(encoder);

  size_t output_size = 6 + 1;
  std::vector<unsigned char> output_buffer(output_size);
  size_t output_size_after_decompression =
      fsst_decompress(&decoder, compressedRowLens[0], compressedRowPtrs[0], output_size, output_buffer.data());

  std::cout << output_size_after_decompression << std::endl;

  for (size_t i = 0; i < output_size_after_decompression; i++) {
    printf("%c", output_buffer[i]);
  }

  printf("\n");

  return 0;
}
