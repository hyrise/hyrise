#include "buffer_manager.hpp"

namespace hyrise {

BufferManager::BufferManager() {
  _ssd_region = std::make_unique<SSDRegion>("/tmp/hyrise.data");
  _volatile_region = std::make_unique<VolatileRegion>();
  _clock_replacement_strategy = std::make_unique<ClockReplacementStrategy>(10);
}

BufferManager::BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region)
    : _ssd_region(std::move(ssd_region)), _volatile_region(std::move(volatile_region)) {}

}  // namespace hyrise