#include "merkledag_decoder.hpp"

#include <libp2p/multi/content_identifier_codec.hpp>
#include <proto/merkledag.pb.h>

OUTCOME_CPP_DEFINE_CATEGORY( sgns::ipfs_bitswap::merkledag, DecodeError, e )
{
    using sgns::ipfs_bitswap::merkledag::DecodeError;
    switch ( e )
    {
        case DecodeError::INVALID_PROTOBUF:
            return "invalid DAG-PB protobuf";
        case DecodeError::MISSING_LINK_CID:
            return "DAG-PB link is missing its CID";
        case DecodeError::INVALID_LINK_CID:
            return "DAG-PB link contains an invalid CID";
    }
    return "unknown DAG-PB decode error";
}

namespace sgns::ipfs_bitswap::merkledag
{
    libp2p::outcome::result<Node> Decode( const std::string &data )
    {
        ::merkledag::pb::PBNode pbNode;
        if ( !pbNode.ParseFromString( data ) )
        {
            return DecodeError::INVALID_PROTOBUF;
        }

        Node node;
        if ( pbNode.has_data() )
        {
            const auto &pbData = pbNode.data();
            node.data.emplace( pbData.begin(), pbData.end() );
        }

        node.links.reserve( pbNode.links_size() );
        for ( const auto &pbLink : pbNode.links() )
        {
            if ( !pbLink.has_hash() )
            {
                return DecodeError::MISSING_LINK_CID;
            }

            const auto &hash = pbLink.hash();
            auto        cid  = libp2p::multi::ContentIdentifierCodec::decode(
                gsl::span( reinterpret_cast<const uint8_t *>( hash.data() ), hash.size() ) );
            if ( !cid )
            {
                return DecodeError::INVALID_LINK_CID;
            }

            node.links.push_back( DecodedLink{
                pbLink.has_name() ? std::make_optional( pbLink.name() ) : std::nullopt,
                std::move( cid.value() ),
                pbLink.has_tsize() ? std::make_optional( pbLink.tsize() ) : std::nullopt } );
        }

        return node;
    }
}
