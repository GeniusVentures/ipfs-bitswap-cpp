#include "bitswap_message.hpp"

#include <string>
#include <tuple>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <boost/assert.hpp>

namespace sgns::ipfs_bitswap
{
    BitswapMessage::BitswapMessage(bitswap_pb::Message& pb_msg)
        : pb_msg_(pb_msg)
    {
    }

    void BitswapMessage::AddWantlistEntry(const libp2p::multi::ContentIdentifier& cid, bool wantBlock)
    {
        auto wantlist = pb_msg_.mutable_wantlist();
        auto entry = wantlist->add_entries();
        auto encodedCID = libp2p::multi::ContentIdentifierCodec::encode(cid).value();
        entry->set_block(encodedCID.data(), encodedCID.size());
        entry->set_priority(1);
        entry->set_cancel(false);
        entry->set_wanttype(wantBlock ? bitswap_pb::Message_Wantlist_WantType_Block
            : bitswap_pb::Message_Wantlist_WantType_Have);
        entry->set_senddonthave(false);
    }

    size_t BitswapMessage::GetWantlistSize() const
    {
        return (pb_msg_.has_wantlist() ? pb_msg_.wantlist().entries_size() : 0);
    }

    const bitswap_pb::Message::Wantlist::Entry& BitswapMessage::GetWantlistEntry(size_t entryIdx) const
    {
        return pb_msg_.wantlist().entries(entryIdx);
    }

    size_t BitswapMessage::GetBlocksSize() const
    {
        return pb_msg_.blocks_size();
    }

    const std::string& BitswapMessage::GetBlock(size_t blockIdx) const
    {
        return pb_msg_.blocks(blockIdx);
    }

    void BitswapMessage::AddBlockPresence(const libp2p::multi::ContentIdentifier& cid, bool have)
    {
        auto presence = pb_msg_.add_blockpresences();
        auto encodedCID = libp2p::multi::ContentIdentifierCodec::encode(cid).value();
        presence->set_cid(encodedCID.data(), encodedCID.size());
        presence->set_type(have ? bitswap_pb::Message_BlockPresenceType_Have : bitswap_pb::Message_BlockPresenceType_DontHave);
    }

}  // namespace sgns::ipfs_bitswap
