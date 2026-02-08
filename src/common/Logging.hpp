#pragma once

#include <mutex>
#include <string>
#include <string_view>

#include "quill/Backend.h"
#include "quill/Frontend.h"
#include "quill/LogMacros.h"
#include "quill/Logger.h"
#include "quill/sinks/ConsoleSink.h"
#include "quill/sinks/FileSink.h"

namespace replay {

inline quill::Logger* initLogger(std::string_view name = "replay",
                                 std::string_view file_path = {}) {
  static std::once_flag once;
  static quill::Logger* logger = nullptr;

  std::call_once(once, [&]() {
    quill::Backend::start();

    if (!file_path.empty()) {
      quill::FileSinkConfig cfg;
      auto sink = quill::Frontend::create_or_get_sink<quill::FileSink>(
          std::string(file_path), cfg);
      logger = quill::Frontend::create_or_get_logger(std::string(name),
                                                     std::move(sink));
    } else {
      auto sink = quill::Frontend::create_or_get_sink<quill::ConsoleSink>(
          "console_sink");
      logger = quill::Frontend::create_or_get_logger(std::string(name),
                                                     std::move(sink));
    }

    logger->set_log_level(quill::LogLevel::Info);
  });

  return logger;
}

inline quill::Logger* logger() { return initLogger(); }

}  // namespace replay
