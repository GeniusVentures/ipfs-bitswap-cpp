include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/src
    )

add_subdirectory(src)

# Add our content retrieval test
find_package(Boost REQUIRED COMPONENTS system filesystem)

add_executable(bitswap_content_test
    bitswap_content_test.cpp
)

target_link_libraries(bitswap_content_test
    ipfs-bitswap-cpp
    ipfs-bitswap-proto
    ipfs-unixfs-proto
    spdlog::spdlog
    Boost::Boost.DI
    p2p::p2p_basic_host
    p2p::p2p_multiaddress
    p2p::p2p_ping
    p2p::p2p_identify
    p2p::p2p_identify_proto
    p2p::p2p_default_host
    p2p::p2p_default_network
    p2p::p2p_security_noise
    p2p::p2p_transport_tcp
    p2p::p2p_muxer_yamux
    ipfs-lite-cpp::ipld_node
    ${WIN_CRYPT_LIBRARY}
    ${Boost_LIBRARIES}
)

target_include_directories(bitswap_content_test PRIVATE
    ${GSL_INCLUDE_DIR}
)

set_target_properties(bitswap_content_test PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Add test to CTest
enable_testing()
add_test(NAME BitswapContentRetrievalTest COMMAND bitswap_content_test)
