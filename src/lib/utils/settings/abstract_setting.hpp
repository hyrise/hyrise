#pragma once

namespace opossum {

/**
 * This is an abstract class for all settings objects.
 *
 * Settings register itself at the SettingsManager.
 * Therefore it is neccessary that they are owned by a shared pointer (instantiate with std::make_shared).
 * Thus, you need to call unenroll() when the setting is not longer needed (it won't get destructed otherwise,
 * as SettingsManager would still hold a shared pointer to it).
 *
 * Settings provide a getter/setter with a string.
 * When the set(...) method ia called, a setting can invoke a method of its parent that triggers
 * the immediate appliance of the changed value.
 *
 * Settings have a unique name that consists of the name of its parent and the setting
 * name (e.g. "TaskScheduler.workers")
 */
class AbstractSetting : public Noncopyable, public std::enable_shared_from_this<AbstractSetting> {
 public:
  explicit AbstractSetting(const std::string& init_name);

  virtual ~AbstractSetting() = default;

  virtual const std::string& description() const = 0;

  virtual const std::string& get() = 0;

  virtual void set(const std::string& value) = 0;

  void enroll();

  void unenroll();

  const std::string& name;
};

}  // namespace opossum
