#include "pos_list_utils.hpp"

namespace opossum {

std::shared_ptr<std::vector<std::shared_ptr<PosList>>> split_pos_list(const std::shared_ptr<PosList>& pos_list,
                                                                      size_t number_of_parts) {
  // Adapted from https://stackoverflow.com/a/37708514
  auto pos_list_out = std::make_shared<std::vector<std::shared_ptr<PosList>>>();

  size_t length = pos_list->size() / number_of_parts;
  size_t remain = pos_list->size() % number_of_parts;

  size_t begin = 0;
  size_t end = 0;

  for (size_t i = 0; i < std::min(number_of_parts, pos_list->size()); ++i) {
    end += (remain > 0) ? (length + ((remain--) != 0)) : length;

    auto part_pos_list = std::make_shared<PosList>(pos_list->begin() + begin, pos_list->begin() + end);
    pos_list_out->push_back(part_pos_list);

    begin = end;
  }

  return pos_list_out;
}

}  // namespace opossum
