#pragma once

#include <ctime>
#include <iomanip>
#include <iostream>

// Minimal formatting of log messages with optimized (= not in production mode) debug log
// This 10 line solution could probably be replaced by boost functionality at some point,
// although you have to link the boost library for that.
// Sticking to the guideline "do not catch errors", a LOG_ERROR is not implemented as we expect errors to be thrown.

#define __LOG_TIME                                                 \
  {                                                                \
    auto time = std::time(nullptr);                                \
    std::cout << std::put_time(std::localtime(&time), "%H:%M:%S"); \
  }

#if IS_DEBUG
#define LOG_DEBUG(msg) \
  __LOG_TIME;          \
  std::cout << " [DEBUG] " << msg << std::endl;
#else
#define LOG_DEBUG(msg)
#endif

#define LOG_INFO(msg) \
  __LOG_TIME;         \
  std::cout << " [INFO]  " << msg << "\n"

#define LOG_WARN(msg) \
  __LOG_TIME;         \
  std::cout << " [WARN]  " << msg << "\n"
