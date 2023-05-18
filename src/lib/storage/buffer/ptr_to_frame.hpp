#include <vector>

struct PtrToFrame {
  struct Region {
    void* start;
    void* end;
  };

  void find_frame_and_offset(void* from) {
    for (int i = 0; i < _regions.size(); i++) {
      auto in_region = _regions[i].start <= from & from < _regions[i].end;
      in_region << i;
    }
  }

  std::vector<Region> _regions;
};