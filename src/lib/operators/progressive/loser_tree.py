#!/usr/bin/env python3

import math
import random
import sys

import dataclasses


LIST_COUNT = 8
ITEMS_PER_LIST = 4

random.seed(17)


@dataclasses.dataclass
class TreeItem:
  value: int = sys.maxsize
  list_index: int = -1  # index into lists
  is_set: bool = False  # TODO: remove, use -1 list_index.


class LoserTree:
  lists = None
  list_count = None
  element_count = None
  array = None
  list_offsets = None
  def __init__(self, init_list_count):
    self.list_count = init_list_count
    self.level_count = math.ceil(math.log2(self.list_count))
    self.element_count = pow(2, self.level_count + 1)
    self.array = [TreeItem()] * self.element_count
    self.list_offsets = [-1] * init_list_count
    print(self.level_count)
    print(self.element_count)
    print([entry.value for entry in self.array])
    print(self.list_offsets)

  def initialize(self, lists):
    self.lists = lists

    def set(level, index):
      if level == self.level_count:
        list_index = index - self.list_count
        assert index >= (self.element_count / 2), "Unexpected index of " + str(index) + "."
        self.array[index] = TreeItem(lists[list_index][0], list_index, True)
        self.list_offsets[list_index] += 1
        return self.array[index]

      left_index = index*2
      right_index = index*2+1

      if not self.array[index*2].is_set:
        left_winner = set(level + 1, left_index)

      if not self.array[index*2+1].is_set:
        right_winner = set(level + 1, right_index)

      if left_winner.value <= right_winner.value:
        self.array[index] = right_winner
        return left_winner
      else:
        self.array[index] = left_winner
        return right_winner

    self.array[0] = set(0, 1)
    print([entry.value for entry in self.array])


  # In case we cannot pull from the lists (exhausted). We create a maxValue default value.
  def get_min_and_pull_next(self):
    ret = dataclasses.replace(self.array[0])
    self.array[0] = TreeItem()

    list_to_load_from = ret.list_index

    list_item_to_pull = None
    new_pulled_value = None

    self.list_offsets[list_to_load_from] += 1
    list_item_to_pull = self.list_offsets[list_to_load_from]
    # print("test", self.list_offsets[list_to_load_from], "==", len(self.lists[list_to_load_from]))
    if self.list_offsets[list_to_load_from] == len(self.lists[list_to_load_from]):
      print("last item")
      list_item_to_pull = TreeItem()
    else:
      new_pulled_value = TreeItem(self.lists[list_to_load_from][list_item_to_pull], list_to_load_from, True)
    

    array_index = self.list_count + list_to_load_from
    
    
    # print("Pulled from lists", new_pulled_value)
    self.array[array_index] = new_pulled_value

    next_index = int(array_index / 2)
    while next_index > -1:
      if next_index == 0:
        # print("replacing head")
        self.array[0] = new_pulled_value
        break

      if new_pulled_value.value > self.array[next_index].value:
        prev_loser = dataclasses.replace(self.array[next_index])
        self.array[next_index] = new_pulled_value
        new_pulled_value = prev_loser
      next_index = int(next_index / 2)

    # print(f"will return {ret} and set to index {array_index} the value of {self.array[array_index]}")

    print([entry.value for entry in self.array])
    return ret



if __name__ == '__main__':
  lists = []

  for list_id in range(LIST_COUNT):
    lists.append(sorted([random.randint(1,20) for _ in range(ITEMS_PER_LIST)]))

  print(lists)

  all_merged = sorted([el for l in lists for el in l])
  print("merged", all_merged)

  tree = LoserTree(8)
  tree.initialize(lists)

  loser_tree_list = []

  for _ in range(30):
    loser_tree_list.append(tree.get_min_and_pull_next().value)

  print("loser_tree_list", loser_tree_list)

  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.get_min_and_pull_next())
  # print(tree.array)
