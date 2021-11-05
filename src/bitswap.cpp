#include "bitswap.hpp"

#include <proto/bitswap.pb.h>

#include <string>
#include <tuple>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <boost/assert.hpp>

namespace
{
    const std::string bitswapProtocolId = "/ipfs/bitswap/1.0.0";
}  // namespace

namespace sgns::ipfs_bitswap {
    Bitswap::Bitswap(libp2p::Host& host,
        libp2p::event::Bus& event_bus)
        : host_{ host }
        , bus_{ event_bus } 
    {
        incoming_.resize(1024);
    }


    libp2p::peer::Protocol Bitswap::getProtocolId() const
    {
        return bitswapProtocolId;
    }

    void Bitswap::handle(libp2p::protocol::BaseProtocol::StreamResult rstream)
    {
        if (!rstream)
        {
            return;
        }

        auto& stream = rstream.value();
        logger_->debug("accepted stream from peer: {}, {}, {}, isInit: {}, isClosed: {}, read: {}, write: {}",
            stream->remotePeerId().value().toBase58(),
            stream->remoteMultiaddr().value().getStringAddress(),
            stream->localMultiaddr().value().getStringAddress(),
            stream->isInitiator().has_failure() ? false : stream->isInitiator().value(),
            stream->isClosed(),
            !stream->isClosedForRead(),
            !stream->isClosedForWrite());

        // Current yamux stream implementation allows to read pending data from a stream that is
        // closed for read.
        //bool isStreamClosedForRead = stream->isClosedForRead();
        bool isStreamClosedForRead = false;

        if (!isStreamClosedForRead)
        {
            auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>(stream);
            rw->read<bitswap_pb::Message>(
                [ctx = shared_from_this()](libp2p::outcome::result<bitswap_pb::Message> rmsg) {
                    if (!rmsg)
                    {
                        ctx->logger_->error("message didn't parsed");
                        return;
                    }
                    auto& msg = rmsg.value();

                    ctx->logger_->debug("wantlist size {}", msg.wantlist().entries_size());

                    for (int i = 0; i < msg.wantlist().entries_size(); ++i)
                    {
                        auto blockId = msg.wantlist().entries(i).block();
                        auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)blockId.data(), blockId.size()));
                        auto scid = libp2p::peer::PeerId::fromHash(cid.value().content_address).value().toBase58();
                        ctx->logger_->debug(scid);
                    }         
                    for (int blockIdx = 0; blockIdx < msg.blocks_size(); ++blockIdx)
                    {
                        ctx->logger_->debug("Block: {}", msg.blocks(blockIdx));
                    }
                });
        }
    }

    void Bitswap::start() {
        // no double starts
        BOOST_ASSERT(!started_);
        started_ = true;

        host_.setProtocolHandler(
            bitswapProtocolId,
            [wp = weak_from_this()](libp2p::protocol::BaseProtocol::StreamResult rstream) {
            if (auto self = wp.lock()) {
                self->handle(std::move(rstream));
            }
        });

        sub_ = bus_.getChannel<libp2p::network::event::OnNewConnectionChannel>().subscribe(
            [wp = weak_from_this()](auto&& conn) {
            if (auto self = wp.lock()) {
                return self->onNewConnection(conn);
            }
        });
    }

    void Bitswap::onNewConnection(
        const std::weak_ptr<libp2p::connection::CapableConnection>& conn)
    {
        if (conn.expired())
        {
            return;
        }

        auto remote_peer_res = conn.lock()->remotePeer();
        if (!remote_peer_res)
        {
            return;
        }

        auto remote_peer_addr_res = conn.lock()->remoteMultiaddr();
        if (!remote_peer_addr_res)
        {
            return;
        }

        libp2p::peer::PeerInfo peer_info
        {
            std::move(remote_peer_res.value()),
            std::vector<libp2p::multi::Multiaddress>{ std::move(remote_peer_addr_res.value())} };

        logger_->debug("connected to peer {}", remote_peer_res.value().toBase58());
        //host_.newStream(
        //    peer_info, bitswapProtocolId,
        //    [self{ shared_from_this() }](auto&& stream_res) {
        //    if (!stream_res) {
        //        return;
        //    }
        //});
    }

    void Bitswap::sendRequest(
        libp2p::protocol::BaseProtocol::StreamResult rstream,
        const libp2p::multi::ContentIdentifier& cid)
    {
        auto& stream = rstream.value();
        logger_->debug("stream to peer: {}, {}, {}, isInit: {}, isClosed: {}, read: {}, write: {}",
            stream->remotePeerId().value().toBase58(),
            stream->remoteMultiaddr().value().getStringAddress(),
            stream->localMultiaddr().value().getStringAddress(),
            stream->isInitiator().has_failure() ? false : stream->isInitiator().value(),
            stream->isClosed(),
            !stream->isClosedForRead(),
            !stream->isClosedForWrite());

        bitswap_pb::Message pb_msg;
        auto wantlist = pb_msg.mutable_wantlist();
        auto entry = wantlist->add_entries();
        //entry->set_block(libp2p::multi::ContentIdentifierCodec::toString(cid).value());
        auto encodedCID = libp2p::multi::ContentIdentifierCodec::encode(cid).value();
        entry->set_block(encodedCID.data(), encodedCID.size());
        entry->set_priority(1);
        entry->set_cancel(false);
        entry->set_wanttype(bitswap_pb::Message_Wantlist_WantType_Block);
        entry->set_senddonthave(false);

        wantlist->set_full(true);
        pb_msg.set_pendingbytes(0);

        auto entries_size = pb_msg.wantlist().entries_size();

        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>(stream);
        rw->write<bitswap_pb::Message>(
            pb_msg,
            [ctx = shared_from_this(),
            stream = std::move(stream)](auto&& written_bytes) mutable {

            ctx->messageSent(written_bytes, std::move(stream));
        });
    }

    void Bitswap::messageSent(
        libp2p::outcome::result<size_t> written_bytes, std::shared_ptr<libp2p::connection::Stream> stream) {
        if (!written_bytes)
        {
            logger_->error("cannot write bitswap message to stream to peer: {}", written_bytes.error().message());
            return stream->reset();
        }

        logger_->info("successfully written a bitswap message message to peer: {}", written_bytes.value());

        stream->close([ctx = shared_from_this()](auto&& res)
        {
            if (!res)
            {
                ctx->logger_->error("cannot close the stream to peer: {}", res.error().message());
            }
            else
            {
                ctx->logger_->debug("stream closed");
            }
        });
    }

    void Bitswap::RequestBlock(
        const libp2p::peer::PeerId& peer,
        boost::optional<libp2p::multi::Multiaddress> address,
        const libp2p::multi::ContentIdentifier& cid)
    {
        libp2p::peer::PeerInfo pi{ peer, {address.value()} };

        // Check if connectable
        auto connectedness = host_.connectedness(pi);
        if (connectedness == libp2p::Host::Connectedness::CAN_NOT_CONNECT)
        {
            logger_->debug("Peer {} is not connectible", pi.id.toBase58());
        }

        host_.newStream(
            pi,
            bitswapProtocolId,
            [wp = weak_from_this(), cid](libp2p::protocol::BaseProtocol::StreamResult rstream)
        {
            auto ctx = wp.lock();
            if (ctx)
            {
                if (!rstream)
                {
                    ctx->logger_->error("no new stream created");
                }
                else
                {
                    ctx->sendRequest(std::move(rstream), cid);
                }
            }
        });
    }
}  // namespace sgns::ipfs_bitswap
