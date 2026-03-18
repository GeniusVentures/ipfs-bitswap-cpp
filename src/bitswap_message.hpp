#pragma once

#include "logger.hpp"

#include <memory>
#include <proto/bitswap.pb.h>

#include <libp2p/multi/content_identifier.hpp>

namespace sgns::ipfs_bitswap
{
    class BitswapMessage
    {
    public:
        BitswapMessage( bitswap_pb::Message &pb_msg );

        void   AddWantlistEntry( const libp2p::multi::ContentIdentifier &cid, bool wantBlock );
        size_t GetWantlistSize() const;
        const bitswap_pb::Message::Wantlist::Entry &GetWantlistEntry( size_t entryIdx ) const;

        size_t             GetBlocksSize() const;
        const std::string &GetBlock( size_t blockIdx ) const;

        void AddBlockPresence( const libp2p::multi::ContentIdentifier &cid, bool have );

    private:
        bitswap_pb::Message &pb_msg_;
        Logger               logger_ = createLogger( "BitswapMessage" );
    };
}
