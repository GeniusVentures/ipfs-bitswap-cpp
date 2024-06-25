
add_library(ipfs-bitswap-cpp
    bitswap.hpp
    bitswap.cpp
    bitswap_message.cpp
    bitswap_message.hpp
    logger.cpp
    logger.hpp
    )

add_proto_library(ipfs-bitswap-proto proto/bitswap.proto)

target_include_directories(ipfs-bitswap-cpp 
    PRIVATE ${GSL_INCLUDE_DIR}
)

target_link_libraries(ipfs-bitswap-cpp    
    ipfs-bitswap-proto
    spdlog::spdlog
    OpenSSL::Crypto
    OpenSSL::SSL
    Boost::Boost.DI
    p2p::p2p_basic_host
    p2p::p2p_multiaddress
    p2p::p2p_ping
    ${WIN_CRYPT_LIBRARY}
    )

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    )
                         