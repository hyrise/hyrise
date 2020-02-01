#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "storage/pos_list.hpp"

namespace opossum {

// TODO Add comment
std::shared_ptr<std::vector<std::shared_ptr<PosList>>> split_pos_list(const std::shared_ptr<PosList>& pos_list, size_t number_of_parts);

}  // namespace opossum
