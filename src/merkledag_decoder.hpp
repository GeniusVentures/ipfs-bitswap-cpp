#pragma once

#include <vector>
#include <string>
#include <map>
#include <optional>
#include <libp2p/multi/content_identifier.hpp>
#include <proto/merkledag.pb.h>

namespace sgns::ipfs_bitswap {

    struct DecodedLink {
        std::string name;
        libp2p::multi::ContentIdentifier cid;
        uint64_t size;
        
        DecodedLink(const std::string& n, const libp2p::multi::ContentIdentifier& c, uint64_t s)
            : name(n), cid(c), size(s) {}
    };

    class MerkledagDecoder {
    public:
        /**
         * Decode a MerkleDAG node from raw bytes
         * @param data - raw protobuf-encoded MerkleDAG node data
         * @return true if decoding succeeded
         */
        bool decode(const std::vector<uint8_t>& data);
        
        /**
         * Decode a MerkleDAG node from string
         * @param data - raw protobuf-encoded MerkleDAG node data
         * @return true if decoding succeeded
         */
        bool decode(const std::string& data);
        
        /**
         * Get the data field from the decoded node
         * @return optional data field (UnixFS data)
         */
        std::optional<std::vector<uint8_t>> getData() const;
        
        /**
         * Get all links from the decoded node
         * @return vector of decoded links
         */
        std::vector<DecodedLink> getLinks() const;
        
        /**
         * Get a specific link by name
         * @param name - link name to search for
         * @return optional decoded link
         */
        std::optional<DecodedLink> getLink(const std::string& name) const;
        
        /**
         * Check if decoding was successful
         * @return true if node was successfully decoded
         */
        bool isValid() const;
        
        /**
         * Get the raw protobuf node (for debugging)
         * @return reference to decoded protobuf node
         */
        const merkledag::pb::PBNode& getNode() const;
        
    private:
        merkledag::pb::PBNode node_;
        bool valid_ = false;
    };

}
