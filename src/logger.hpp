#ifndef IPFS_BITSWAP_LOGGER_HPP
#define IPFS_BITSWAP_LOGGER_HPP

#include <spdlog/spdlog.h>

namespace sgns::ipfs_bitswap
{
    using Logger = std::shared_ptr<spdlog::logger>;
    /**
    * Provide logger object
    * @param tag - tagging name for identifying logger
    * @return logger object
    */
    Logger createLogger(const std::string& tag);
}

#endif  // IPFS_BITSWAP_LOGGER_HPP
