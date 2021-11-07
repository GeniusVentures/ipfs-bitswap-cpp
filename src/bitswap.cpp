#include "bitswap.hpp"

#include "bitswap_message.hpp"

#include <string>
#include <tuple>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <boost/assert.hpp>

OUTCOME_CPP_DEFINE_CATEGORY_3(sgns::ipfs_bitswap, Bitswap::BitswapError, e) 
{
    return "Bitswap::BitswapError";
}

namespace
{
    const std::string bitswapProtocolId = "/ipfs/bitswap/1.0.0";
}  // namespace

namespace sgns::ipfs_bitswap {
    Bitswap::Bitswap(libp2p::Host& host,
        libp2p::event::Bus& eventBus)
        : host_{ host }
        , bus_{ eventBus } 
    {
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
        logger_->debug("accepted stream from peer: {}, {}, {}, isClosed: {}, canRead: {}, canWrite: {}",
            stream->remotePeerId().value().toBase58(),
            stream->remoteMultiaddr().value().getStringAddress(),
            stream->localMultiaddr().value().getStringAddress(),
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
                        ctx->logger_->error("bitswap message cannot be decoded");
                        return;
                    }

                    BitswapMessage msg(rmsg.value());

                    ctx->logger_->debug("wantlist size {}", msg.GetWantlistSize());

                    for (int i = 0; i < msg.GetWantlistSize(); ++i)
                    {
                        auto blockId = msg.GetWantlistEntry(i).block();
                        auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)blockId.data(), blockId.size()));
                        ctx->logger_->debug(libp2p::multi::ContentIdentifierCodec::toString(cid.value()).value());
                    }

                    for (int blockIdx = 0; blockIdx < msg.GetBlocksSize(); ++blockIdx)
                    {
                        const auto& block = msg.GetBlock(blockIdx);
                        ctx->logger_->debug("Block: {}", block);

                        auto cidV0 = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(block.data(), block.size());
                        auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)cidV0.data(), cidV0.size()));
                        if (!cid)
                        {
                            ctx->logger_->error("CID cannot be decoded. {}", cid.error().message());
                        }
                        else
                        {
                            auto scid = libp2p::multi::ContentIdentifierCodec::toString(cid.value()).value();
                            ctx->logger_->debug("Block CID: {}", scid);
                        }

                        // @todo Get CID and call callbacks related to the CID
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
    }

    void Bitswap::sendRequest(
        std::shared_ptr<libp2p::connection::Stream> stream,
        const libp2p::multi::ContentIdentifier& cid,
        BlockCallback onBlockCallback)
    {
        bitswap_pb::Message pb_msg;
        BitswapMessage msg(pb_msg);
        msg.AddWantlistEntry(cid, true);

        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>(stream);
        rw->write<bitswap_pb::Message>(
            pb_msg,
            [ctx = shared_from_this(),
            stream = std::move(stream),
            onBlockCallback = std::move(onBlockCallback)](auto&& writtenBytes) mutable {

            ctx->messageSent(writtenBytes, std::move(stream), std::move(onBlockCallback));
        });
    }

    void Bitswap::messageSent(
        libp2p::outcome::result<size_t> writtenBytes,
        std::shared_ptr<libp2p::connection::Stream> stream,
        BlockCallback onBlockCallback)
    {
        if (!writtenBytes)
        {
            logger_->error("cannot write bitswap message to stream to peer: {}", writtenBytes.error().message());
            stream->reset();
            onBlockCallback(BitswapError::MESSAGE_SENDING_FAILURE);
            return;
        }

        logger_->info("successfully written a bitswap message message to peer: {}", writtenBytes.value());

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
        const CID& cid,
        BlockCallback onBlockCallback)
    {
        libp2p::peer::PeerInfo pi{ peer, {address.value()} };

        // Check if connectable
        auto connectedness = host_.connectedness(pi);
        if (connectedness == libp2p::Host::Connectedness::CAN_NOT_CONNECT)
        {
            logger_->debug("Peer {} is not connectible", pi.id.toBase58());
            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
            return;
        }

        host_.newStream(
            pi,
            bitswapProtocolId,
            [wp = weak_from_this(), cid, onBlockCallback]
                (libp2p::protocol::BaseProtocol::StreamResult rstream)
        {
            auto ctx = wp.lock();
            if (ctx)
            {
                if (!rstream)
                {
                    ctx->logger_->error("no new stream created");
                    onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
                }
                else
                {
                    auto stream = rstream.value();
                    ctx->logger_->debug("outbound stream to peer: {}, {}, {}, isClosed: {}, canRead: {}, canWrite: {}",
                        stream->remotePeerId().value().toBase58(),
                        stream->remoteMultiaddr().value().getStringAddress(),
                        stream->localMultiaddr().value().getStringAddress(),
                        stream->isClosed(),
                        !stream->isClosedForRead(),
                        !stream->isClosedForWrite());
                    ctx->sendRequest(std::move(stream), cid, onBlockCallback);
                }
            }
        });
    }
}  // namespace sgns::ipfs_bitswap
