#include "merkledag_decoder.hpp"
#include "merkledag_decoder.hpp"
#include <proto/merkledag.pb.h>
#include <libp2p/multi/content_identifier_codec.hpp>

namespace sgns::ipfs_bitswap
{

    bool MerkledagDecoder::decode( const std::vector<uint8_t> &data )
    {
        valid_ = node_.ParseFromArray( data.data(), static_cast<int>( data.size() ) );
        return valid_;
    }

    bool MerkledagDecoder::decode( const std::string &data )
    {
        valid_ = node_.ParseFromString( data );
        return valid_;
    }

    std::optional<std::vector<uint8_t>> MerkledagDecoder::getData() const
    {
        if ( !valid_ || !node_.has_data() )
        {
            return std::nullopt;
        }

        const std::string &dataStr = node_.data();
        return std::vector<uint8_t>( dataStr.begin(), dataStr.end() );
    }

    std::vector<DecodedLink> MerkledagDecoder::getLinks() const
    {
        std::vector<DecodedLink> links;
        if ( !valid_ )
        {
            return links;
        }

        for ( const auto &pbLink : node_.links() )
        {
            // Decode the CID from the hash field
            const std::string &hashStr   = pbLink.hash();
            auto               cidResult = libp2p::multi::ContentIdentifierCodec::decode(
                gsl::span( reinterpret_cast<const uint8_t *>( hashStr.data() ), hashStr.size() ) );
            if ( cidResult.has_value() )
            {
                links.emplace_back( pbLink.name(), std::move( cidResult.value() ), pbLink.tsize() );
            }
        }

        return links;
    }

    std::optional<DecodedLink> MerkledagDecoder::getLink( const std::string &name ) const
    {
        if ( !valid_ )
        {
            return std::nullopt;
        }

        for ( const auto &pbLink : node_.links() )
        {
            if ( pbLink.name() == name )
            {
                const std::string &hashStr   = pbLink.hash();
                auto               cidResult = libp2p::multi::ContentIdentifierCodec::decode(
                    gsl::span( reinterpret_cast<const uint8_t *>( hashStr.data() ), hashStr.size() ) );
                if ( cidResult.has_value() )
                {
                    return DecodedLink( pbLink.name(), std::move( cidResult.value() ), pbLink.tsize() );
                }
            }
        }

        return std::nullopt;
    }

    bool MerkledagDecoder::isValid() const
    {
        return valid_;
    }

    const merkledag::pb::PBNode &MerkledagDecoder::getNode() const
    {
        return node_;
    }

}
