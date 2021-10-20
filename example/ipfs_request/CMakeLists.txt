add_executable(ipfs_request_example
    ipfs.cpp
    )


# Copy the required headers to install directory
include_directories(
    ${PROJECT_SOURCE_DIR}/src
    )

target_include_directories(ipfs_request_example PRIVATE ${GSL_INCLUDE_DIR})


target_link_libraries(ipfs_request_example
#    ipfs-lite-cpp::ipfs_datastore_rocksdb
#    ipfs-lite-cpp::buffer
#    ipfs-lite-cpp::ipld_node
#    ipfs-lite-cpp::ipfs_merkledag_service
    ipfs-bitswap-cpp
    p2p::p2p_basic_host
    p2p::p2p_default_network
    p2p::p2p_peer_repository
    p2p::p2p_inmem_address_repository
    p2p::p2p_inmem_key_repository
    p2p::p2p_inmem_protocol_repository
    p2p::p2p_literals
    p2p::p2p_kademlia
    p2p::p2p_identify
    Boost::Boost.DI
    #Boost::program_options    
    Boost::date_time
    Boost::regex
    ${WIN_CRYPT_LIBRARY}
    )
