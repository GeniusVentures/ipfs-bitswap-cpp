
add_library(ipfs-bitswap-cpp
    bitswap.hpp
    bitswap.cpp
    logger.cpp
    logger.hpp
    )

add_proto_library(ipfs-bitswap-proto proto/bitswap.proto)

target_include_directories(ipfs-bitswap-cpp 
    PRIVATE ${GSL_INCLUDE_DIR}
)

target_link_libraries(ipfs-bitswap-cpp    
    ipfs-bitswap-proto
    OpenSSL::Crypto
    OpenSSL::SSL
    Boost::Boost.DI
    p2p::p2p_basic_host
    p2p::p2p_multiaddress
    #p2p::p2p_default_network
    #p2p::p2p_peer_repository
    #p2p::p2p_inmem_address_repository
    #p2p::p2p_inmem_key_repository
    #p2p::p2p_inmem_protocol_repository
    p2p::p2p_ping
    #p2p::asio_scheduler
    spdlog::spdlog
    ${WIN_CRYPT_LIBRARY}
    )

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    )
                         