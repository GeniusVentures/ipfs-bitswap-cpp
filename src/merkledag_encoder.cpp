#include "merkledag_encoder.hpp"

namespace sgns::ipfs_bitswap::merkledag
{
    namespace
    {
        std::vector<uint8_t> EncodeVarint( uint64_t value )
        {
            std::vector<uint8_t> result;

            while ( value >= 0x80 )
            {
                result.push_back( static_cast<uint8_t>( ( value & 0x7F ) | 0x80 ) );
                value >>= 7;
            }
            result.push_back( static_cast<uint8_t>( value & 0x7F ) );

            return result;
        }

        uint32_t MakeTag( uint32_t fieldNumber, uint32_t wireType )
        {
            return ( fieldNumber << 3 ) | wireType;
        }

        std::vector<uint8_t> EncodeBytes( uint32_t fieldNumber, const std::vector<uint8_t> &data )
        {
            std::vector<uint8_t> result;

            // Tag: field number + wire type 2 (length-delimited)
            auto tagBytes = EncodeVarint( MakeTag( fieldNumber, 2 ) );
            result.insert( result.end(), tagBytes.begin(), tagBytes.end() );

            // Length
            auto lengthBytes = EncodeVarint( data.size() );
            result.insert( result.end(), lengthBytes.begin(), lengthBytes.end() );

            // Data
            result.insert( result.end(), data.begin(), data.end() );

            return result;
        }

        std::vector<uint8_t> EncodeString( uint32_t fieldNumber, const std::string &str )
        {
            std::vector<uint8_t> data( str.begin(), str.end() );
            return EncodeBytes( fieldNumber, data );
        }

        std::vector<uint8_t> EncodeVarintField( uint32_t fieldNumber, uint64_t value )
        {
            std::vector<uint8_t> result;

            // Tag: field number + wire type 0 (varint)
            auto tagBytes = EncodeVarint( MakeTag( fieldNumber, 0 ) );
            result.insert( result.end(), tagBytes.begin(), tagBytes.end() );

            // Value
            auto valueBytes = EncodeVarint( value );
            result.insert( result.end(), valueBytes.begin(), valueBytes.end() );

            return result;
        }
    }

    std::vector<uint8_t> Encode( const std::string                                 &data,
                                 const std::map<std::string, std::vector<uint8_t>> &links )
    {
        std::vector<uint8_t> result;

        // Manual encoding to match Kubo's exact field order: Links first, then Data

        // Field 2: Links (repeated PBLink) - encode first like Kubo
        for ( const auto &[name, cidBytes] : links )
        {
            std::vector<uint8_t> linkData;

            // PBLink.Hash = 1 (bytes)
            auto hashField = EncodeBytes( 1, cidBytes );
            linkData.insert( linkData.end(), hashField.begin(), hashField.end() );

            // PBLink.Name = 2 (string) - only encode if not empty for directories
            if ( !name.empty() )
            {
                auto nameField = EncodeString( 2, name );
                linkData.insert( linkData.end(), nameField.begin(), nameField.end() );
            }

            // PBLink.Tsize = 3 (uint64) - set to 0 for now (legacy behavior)
            auto sizeField = EncodeVarintField( 3, 0 );
            linkData.insert( linkData.end(), sizeField.begin(), sizeField.end() );

            // Encode the complete link as field 2 (length-delimited)
            auto linkField = EncodeBytes( 2, linkData );
            result.insert( result.end(), linkField.begin(), linkField.end() );
        }

        // Field 1: Data (optional bytes) - encode last like Kubo
        if ( !data.empty() )
        {
            std::vector<uint8_t> dataBytes( data.begin(), data.end() );
            auto                 dataField = EncodeBytes( 1, dataBytes );
            result.insert( result.end(), dataField.begin(), dataField.end() );
        }

        return result;
    }

    std::vector<uint8_t> Encode( const std::string &data, const std::vector<Link> &links )
    {
        std::vector<uint8_t> result;

        // Manual encoding to match Kubo's exact field order: Links first, then Data

        // Field 2: Links (repeated PBLink) - encode first like Kubo
        for ( const auto &link : links )
        {
            std::vector<uint8_t> linkData;

            // PBLink.Hash = 1 (bytes)
            auto hashField = EncodeBytes( 1, link.cid );
            linkData.insert( linkData.end(), hashField.begin(), hashField.end() );

            // PBLink.Name = 2 (string) - always encode to match Kubo (even if empty)
            auto nameField = EncodeString( 2, link.name );
            linkData.insert( linkData.end(), nameField.begin(), nameField.end() );

            // PBLink.Tsize = 3 (uint64)
            auto sizeField = EncodeVarintField( 3, link.tsize );
            linkData.insert( linkData.end(), sizeField.begin(), sizeField.end() );

            // Encode the complete link as field 2 (length-delimited)
            auto linkField = EncodeBytes( 2, linkData );
            result.insert( result.end(), linkField.begin(), linkField.end() );
        }

        // Field 1: Data (optional bytes) - encode last like Kubo
        if ( !data.empty() )
        {
            std::vector<uint8_t> dataBytes( data.begin(), data.end() );
            auto                 dataField = EncodeBytes( 1, dataBytes );
            result.insert( result.end(), dataField.begin(), dataField.end() );
        }

        return result;
    }

}
