#include "logger.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
//#include <spdlog/fmt/ostr.h>

namespace 
{
    void setGlobalPattern(spdlog::logger& logger) 
    {
        logger.set_pattern("[%Y-%m-%d %H:%M:%S.%F] %n %v");
    }

    void setDebugPattern(spdlog::logger& logger) 
    {
        logger.set_pattern("[%Y-%m-%d %H:%M:%S.%F][th:%t][%l] %n %v");
    }

    std::shared_ptr<spdlog::logger> createLogger(const std::string& tag, bool debug_mode = true)
    {
        auto logger = spdlog::stdout_color_mt(tag);
        if (debug_mode)
        {
            setDebugPattern(*logger);
        }
        else
        {
            setGlobalPattern(*logger);
        }
        return logger;
    }
}  // namespace

namespace sgns::ipfs_bitswap
{
    Logger createLogger(const std::string& tag)
    {
        static std::mutex mutex;
        std::lock_guard<std::mutex> lock(mutex);
        auto logger = spdlog::get(tag);
        if (logger == nullptr)
        {
            logger = ::createLogger(tag);
        }
        return logger;
    }
}  // namespace sgns::common
