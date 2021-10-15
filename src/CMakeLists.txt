
add_library(ipfs-bitswap-cpp
    bitswap.cpp
    )

message("Protobuf_PROTOC_EXECUTABLE_3 " ${Protobuf_PROTOC_EXECUTABLE})
add_proto_library(ipfs-bitswap-proto proto/bitswap.proto)

#target_include_directories(ipfs-bitswap PRIVATE ${GSL_INCLUDE_DIR})

target_include_directories(ipfs-bitswap-cpp
    PUBLIC $<BUILD_INTERFACE:${PROTO_SRC_DIR}>
   "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)

target_link_libraries(ipfs-bitswap-cpp    
    #ipfs-bitswap-proto
    #OpenSSL::Crypto
    #OpenSSL::SSL
    #Boost::Boost.DI
    #p2p::p2p_basic_host
    #p2p::p2p_default_network
    #p2p::p2p_peer_repository
    #p2p::p2p_inmem_address_repository
    #p2p::p2p_inmem_key_repository
    #p2p::p2p_inmem_protocol_repository
    #p2p::p2p_gossip
    #p2p::asio_scheduler
    #${WIN_CRYPT_LIBRARY}
    )

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    )
