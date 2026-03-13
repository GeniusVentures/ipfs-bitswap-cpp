#pragma once

#include <map>
#include <string>
#include <vector>
#include <cstdint>
#include <proto/merkledag.pb.h>
namespace sgns::ipfs_bitswap {

/**
 * @brief Represents a MerkleDAG link
 */
struct MerkledagLink {
    std::string name;
    std::vector<uint8_t> cid;
    uint64_t tsize = 0;
};

/**
 * @brief Kubo-compatible MerkleDAG node encoder
 * @details Uses the official go-merkledag protobuf schema for exact CID compatibility with Kubo
 */
class MerkledagEncoder {
public:
    /**
     * @brief Encode a MerkleDAG node using Kubo-compatible protobuf format
     * @param data Node data (UnixFS or raw data)
     * @param links Map of link names to CID bytes
     * @return Protobuf-encoded bytes matching Kubo's format
     */
    static std::vector<uint8_t> encode(const std::string& data, 
                                     const std::map<std::string, std::vector<uint8_t>>& links);
                                     
    /**
     * @brief Encode a MerkleDAG node using Kubo-compatible protobuf format with ordered links
     * @param data Node data (UnixFS or raw data)
     * @param links Vector of ordered links (preserves order and allows duplicate names)
     * @return Protobuf-encoded bytes matching Kubo's format
     */
    static std::vector<uint8_t> encode(const std::string& data, 
                                     const std::vector<MerkledagLink>& links);

private:
    /**
     * @brief Encode a varint value
     */
    static std::vector<uint8_t> encodeVarint(uint64_t value);
    
    /**
     * @brief Encode a length-delimited field (wire type 2)
     */
    static std::vector<uint8_t> encodeBytes(uint32_t fieldNumber, const std::vector<uint8_t>& data);
    
    /**
     * @brief Encode a string field
     */
    static std::vector<uint8_t> encodeString(uint32_t fieldNumber, const std::string& str);
    
    /**
     * @brief Encode a varint field (wire type 0)  
     */
    static std::vector<uint8_t> encodeVarintField(uint32_t fieldNumber, uint64_t value);
    
    /**
     * @brief Create a protobuf field tag
     */
    static uint32_t makeTag(uint32_t fieldNumber, uint32_t wireType);
};

}
