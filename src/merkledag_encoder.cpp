#include "merkledag_encoder.hpp"
#include <proto/merkledag.pb.h>
#include <algorithm>

namespace sgns::ipfs_bitswap {

std::vector<uint8_t> MerkledagEncoder::encode(const std::string& data, 
                                            const std::map<std::string, std::vector<uint8_t>>& links) {
    // Create PBNode using protobuf
    merkledag::pb::PBNode pbNode;
    
    // Add links first (map order) - Kubo serializes Links before Data
    for (const auto& [name, cidBytes] : links) {
        merkledag::pb::PBLink* pbLink = pbNode.add_links();
        
        // Set hash (CID bytes)
        pbLink->set_hash(cidBytes.data(), cidBytes.size());
        
        // Set name only if not empty (protobuf will automatically omit empty strings)
        if (!name.empty()) {
            pbLink->set_name(name);
        }
        
        // Set tsize to 0 for now (legacy behavior)
        pbLink->set_tsize(0);
    }
    
    // Set data field last to match Kubo serialization order
    if (!data.empty()) {
        pbNode.set_data(data);
    }
    
    // Serialize to bytes
    std::string serialized = pbNode.SerializeAsString();
    return std::vector<uint8_t>(serialized.begin(), serialized.end());
}

std::vector<uint8_t> MerkledagEncoder::encode(const std::string& data, 
                                            const std::vector<MerkledagLink>& links) {
    std::vector<uint8_t> result;
    
    // Manual encoding to match Kubo's exact field order: Links first, then Data
    
    // Field 2: Links (repeated PBLink) - encode first like Kubo
    for (const auto& link : links) {
        std::vector<uint8_t> linkData;
        
        // PBLink.Hash = 1 (bytes)
        auto hashField = encodeBytes(1, link.cid);
        linkData.insert(linkData.end(), hashField.begin(), hashField.end());
        
        // PBLink.Name = 2 (string) - always encode to match Kubo (even if empty)
        auto nameField = encodeString(2, link.name);
        linkData.insert(linkData.end(), nameField.begin(), nameField.end());
        
        // PBLink.Tsize = 3 (uint64)
        auto sizeField = encodeVarintField(3, link.tsize);
        linkData.insert(linkData.end(), sizeField.begin(), sizeField.end());
        
        // Encode the complete link as field 2 (length-delimited)
        auto linkField = encodeBytes(2, linkData);
        result.insert(result.end(), linkField.begin(), linkField.end());
    }
    
    // Field 1: Data (optional bytes) - encode last like Kubo
    if (!data.empty()) {
        std::vector<uint8_t> dataBytes(data.begin(), data.end());
        auto dataField = encodeBytes(1, dataBytes);
        result.insert(result.end(), dataField.begin(), dataField.end());
    }
    
    return result;
}

std::vector<uint8_t> MerkledagEncoder::encodeVarint(uint64_t value) {
    std::vector<uint8_t> result;
    
    while (value >= 0x80) {
        result.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    result.push_back(static_cast<uint8_t>(value & 0x7F));
    
    return result;
}

std::vector<uint8_t> MerkledagEncoder::encodeBytes(uint32_t fieldNumber, const std::vector<uint8_t>& data) {
    std::vector<uint8_t> result;
    
    // Tag: field number + wire type 2 (length-delimited)
    uint32_t tag = makeTag(fieldNumber, 2);
    auto tagBytes = encodeVarint(tag);
    result.insert(result.end(), tagBytes.begin(), tagBytes.end());
    
    // Length
    auto lengthBytes = encodeVarint(data.size());
    result.insert(result.end(), lengthBytes.begin(), lengthBytes.end());
    
    // Data
    result.insert(result.end(), data.begin(), data.end());
    
    return result;
}

std::vector<uint8_t> MerkledagEncoder::encodeString(uint32_t fieldNumber, const std::string& str) {
    std::vector<uint8_t> data(str.begin(), str.end());
    return encodeBytes(fieldNumber, data);
}

std::vector<uint8_t> MerkledagEncoder::encodeVarintField(uint32_t fieldNumber, uint64_t value) {
    std::vector<uint8_t> result;
    
    // Tag: field number + wire type 0 (varint)
    uint32_t tag = makeTag(fieldNumber, 0);
    auto tagBytes = encodeVarint(tag);
    result.insert(result.end(), tagBytes.begin(), tagBytes.end());
    
    // Value
    auto valueBytes = encodeVarint(value);
    result.insert(result.end(), valueBytes.begin(), valueBytes.end());
    
    return result;
}

uint32_t MerkledagEncoder::makeTag(uint32_t fieldNumber, uint32_t wireType) {
    return (fieldNumber << 3) | wireType;
}

} // namespace sgns::ipfs_bitswap