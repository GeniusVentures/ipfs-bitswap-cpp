#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace sgns::ipfs_bitswap::merkledag
{
    /**
     * @brief Represents a MerkleDAG link
     */
    struct Link
    {
        std::string          name;
        std::vector<uint8_t> cid;
        uint64_t             tsize = 0;
    };

    /**
     * @brief Encode a MerkleDAG node using Kubo-compatible protobuf format
     * @param data Node data (UnixFS or raw data)
     * @param links Map of link names to CID bytes
     * @return Protobuf-encoded bytes matching Kubo's format
     */
    std::vector<uint8_t> Encode( const std::string                                 &data,
                                 const std::map<std::string, std::vector<uint8_t>> &links );

    /**
     * @brief Encode a MerkleDAG node using Kubo-compatible protobuf format with ordered links
     * @param data Node data (UnixFS or raw data)
     * @param links Vector of ordered links (preserves order and allows duplicate names)
     * @return Protobuf-encoded bytes matching Kubo's format
     */
    std::vector<uint8_t> Encode( const std::string &data, const std::vector<Link> &links );

}
