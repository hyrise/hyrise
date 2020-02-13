#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "storage/pos_list.hpp"

namespace opossum {

/**
 * Splits up a PosList into n PosLists of equal length. If PosList length isn't dividable into n equal parts without
 * remainder, the last PosList will have fewer elements.  
 */
std::shared_ptr<std::vector<std::shared_ptr<PosList>>> split_pos_list(const std::shared_ptr<PosList>& pos_list,
                                                                      size_t number_of_parts);

}  // namespace opossum
