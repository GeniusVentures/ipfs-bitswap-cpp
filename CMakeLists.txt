cmake_minimum_required(VERSION 3.2)

project(ipfs-bitswap-cpp VERSION 0.1)

include(GNUInstallDirs)

message("Protobuf_DIR " ${Protobuf_DIR})
find_package(Protobuf CONFIG REQUIRED)

# libp2p and its dependencies
find_package(libp2p CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# BOOST library
find_package(Boost REQUIRED COMPONENTS random filesystem)

add_subdirectory(src)

# Tests building
find_package(GTest CONFIG REQUIRED)
option(TESTING "Build tests" OFF)
if (TESTING)
    SET(WIN_CRYPT_LIBRARY)
    if (NOT UNIX)
        SET(WIN_CRYPT_LIBRARY crypt32)
    endif()
    include(cmake/test.cmake)
    enable_testing()
    add_subdirectory(test)
endif ()

# Installation setup
set(IPFS_BITSWAP_CONFIG_DESTINATION_DIR "lib/cmake/ipfs-bitswap-cpp")

install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}" FILES_MATCHING  PATTERN "*.hpp")

install(TARGETS ipfs-bitswap-cpp EXPORT ipfs-bitswap-cppTargets DESTINATION "${CMAKE_INSTALL_LIBDIR}")

install(EXPORT ipfs-bitswap-cppTargets
  FILE ipfs-bitswap-cppTargets.cmake
  DESTINATION ${IPFS_BITSWAP_CONFIG_DESTINATION_DIR}
)

include(CMakePackageConfigHelpers)
# generate the config file that is includes the exports
configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/ipfs-bitswap-cppConfig.cmake"
  INSTALL_DESTINATION "lib/cmake/examples"
  NO_SET_AND_CHECK_MACRO
  NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

# generate the version file for the config file
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/ipfs-bitswap-cppConfigVersion.cmake"
  VERSION "${ipfs-bitswap-cpp_VERSION_MAJOR}.${ipfs-bitswap-cpp_VERSION_MINOR}"
  COMPATIBILITY AnyNewerVersion
)

# install the configuration file
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/ipfs-bitswap-cppConfig.cmake
  DESTINATION ${IPFS_BITSWAP_CONFIG_DESTINATION_DIR}
)