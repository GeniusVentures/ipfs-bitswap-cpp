#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <libp2p/multi/content_identifier.hpp>
#include <libp2p/outcome/outcome.hpp>

namespace sgns::ipfs_bitswap::merkledag
{
    enum class DecodeError : uint8_t
    {
        INVALID_PROTOBUF = 1,
        MISSING_LINK_CID,
        INVALID_LINK_CID
    };

    struct DecodedLink
    {
        std::optional<std::string>       name;
        libp2p::multi::ContentIdentifier cid;
        std::optional<uint64_t>          tsize;
    };

    struct Node
    {
        std::optional<std::vector<uint8_t>> data;
        std::vector<DecodedLink>            links;
    };

    /**
     * Decode a DAG-PB node and all of its link CIDs.
     * @param data Raw protobuf-encoded DAG-PB node data.
     * @return A completely decoded node or an error.
     */
    libp2p::outcome::result<Node> Decode( const std::string &data );
}

OUTCOME_HPP_DECLARE_ERROR_2( sgns::ipfs_bitswap::merkledag, DecodeError );
