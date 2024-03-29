#include "bitswap.hpp"

#include "bitswap_message.hpp"

#include <string>
#include <tuple>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <boost/assert.hpp>

OUTCOME_CPP_DEFINE_CATEGORY_3(sgns::ipfs_bitswap, BitswapError, e) 
{
    using sgns::ipfs_bitswap::BitswapError;
    switch (e) {
    case BitswapError::OUTBOUND_STREAM_FAILURE:
        return "failed to create an onbound stream";
    case BitswapError::MESSAGE_SENDING_FAILURE:
        return "cannot send bitswap message";
    case BitswapError::REQUEST_TIMEOUT:
        return "bitswap request timeout";
    }
    return "unknown bitswap error";
}

namespace
{
    const std::string bitswapProtocolId = "/ipfs/bitswap/1.0.0";
}  // namespace

namespace sgns::ipfs_bitswap {
    BitswapRequestContext::BitswapRequestContext(
        boost::asio::io_context& context, const CID& cid)
        : responseTimer_(context)
        , responseTimeout_(boost::posix_time::seconds(5))
    {
    }

    void BitswapRequestContext::AddCallback(BlockCallback callback)
    {
        responseTimer_.expires_from_now(responseTimeout_);
        responseTimer_.async_wait(std::bind(&BitswapRequestContext::HandleResponseTimeout, this));
        callbacks_.emplace_back(std::move(callback));
    }

    void BitswapRequestContext::HandleResponse(libp2p::outcome::result<std::string> block)
    {
        responseTimer_.expires_at(boost::posix_time::pos_infin);
        for (auto& callback : callbacks_)
        {
            callback(block);
        }
        callbacks_.clear();
    }

    void BitswapRequestContext::HandleResponseTimeout()
    {
        HandleResponse(BitswapError::OUTBOUND_STREAM_FAILURE);
    }

    Bitswap::Bitswap(
        libp2p::Host& host,
        libp2p::event::Bus& eventBus,
        std::shared_ptr<boost::asio::io_context> context)
        : host_{ host }
        , bus_{ eventBus }
        , context_(std::move(context))
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
        logStreamState("accepted stream from peer", *stream);

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

                    ctx->logger_->debug("wantlist size: {}", msg.GetWantlistSize());

                    for (int i = 0; i < msg.GetWantlistSize(); ++i)
                    {
                        auto blockId = msg.GetWantlistEntry(i).block();
                        auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)blockId.data(), blockId.size()));
                        ctx->logger_->trace("wantlist item[{}]: {}", i, libp2p::multi::ContentIdentifierCodec::toString(cid.value()).value());
                    }

                    for (int blockIdx = 0; blockIdx < msg.GetBlocksSize(); ++blockIdx)
                    {
                        const auto& block = msg.GetBlock(blockIdx);
                        ctx->logger_->debug("Block received ({} bytes)", block.size());

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

                            std::lock_guard<std::mutex> callbacksGuard(ctx->mutexRequestCallbacks_);
                            auto itContext = ctx->requestContexts_.find(cid.value());
                            if (itContext != ctx->requestContexts_.end())
                            {
                                itContext->second->HandleResponse(block);
                            }
                        }
                    }
                });
        }
    }

    void Bitswap::start() {
        // no double starts allowed
        BOOST_ASSERT(!started_);
        started_ = true;

        host_.setProtocolHandler(
            bitswapProtocolId,
            [wp = weak_from_this()](libp2p::protocol::BaseProtocol::StreamResult rstream) {
            if (auto self = wp.lock()) {
                self->handle(std::move(rstream));
            }
        });

        sub_ = bus_.getChannel<libp2p::event::network::OnNewConnectionChannel>().subscribe(
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

        //auto remote_peer_addr_res = conn.lock()->remoteMultiaddr();
        //if (!remote_peer_addr_res)
        //{
        //    return;
        //}

        //libp2p::peer::PeerInfo peer_info
        //{
        //    std::move(remote_peer_res.value()),
        //    std::vector<libp2p::multi::Multiaddress>{ std::move(remote_peer_addr_res.value())} 
        //};

        logger_->debug("connected to peer {}", remote_peer_res.value().toBase58());
    }

    void Bitswap::writeBitswapMessageToStream(
        std::shared_ptr<libp2p::connection::Stream> stream,
        const CID& cid,
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
            cid(cid),
            onBlockCallback = std::move(onBlockCallback)](auto&& writtenBytes) mutable {
            ctx->messageSent(writtenBytes, std::move(stream), cid, std::move(onBlockCallback));
        });
    }

    void Bitswap::messageSent(
        libp2p::outcome::result<size_t> writtenBytes,
        std::shared_ptr<libp2p::connection::Stream> stream,
        const CID& cid,
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

        std::lock_guard<std::mutex> callbacksGuard(mutexRequestCallbacks_);
        auto itCallbacks = requestContexts_.find(cid);
        if (itCallbacks != requestContexts_.end())
        {
            // A request for the CID has already been sent
            itCallbacks->second->AddCallback(std::move(onBlockCallback));
        }
        else
        {
            auto requestContext = std::make_shared<BitswapRequestContext>(*context_, cid);
            requestContext->AddCallback(std::move(onBlockCallback));
            requestContexts_.emplace(cid, std::move(requestContext));
        }

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
        const libp2p::peer::PeerInfo& pi,
        const CID& cid,
        BlockCallback onBlockCallback)
    {
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
            [wp = weak_from_this(), cid(cid), pi(pi), onBlockCallback = std::move(onBlockCallback)]
                (libp2p::protocol::BaseProtocol::StreamResult rstream) mutable
            {
                auto ctx = wp.lock();
                if (ctx)
                {
                    if (!rstream)
                    {
                        ctx->logger_->error("no new outbound stream created to peer {}", pi.id.toBase58());
                        onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
                    }
                    else
                    {
                        auto stream = rstream.value();
                        ctx->logStreamState("outbound stream created", *stream);
                        ctx->writeBitswapMessageToStream(std::move(stream), cid, std::move(onBlockCallback));
                    }
                }
            },
            // @todo Add the timeout as an input parameter
            std::chrono::milliseconds(1000));
    }

    void Bitswap::logStreamState(const std::string_view& message, libp2p::connection::Stream& stream)
    {
        if (logger_->should_log(spdlog::level::debug))
        {
            logger_->debug("{}: {}, {}, {}, isClosed: {}, canRead: {}, canWrite: {}",
                message,
                stream.remotePeerId().value().toBase58(),
                stream.remoteMultiaddr().value().getStringAddress(),
                stream.localMultiaddr().value().getStringAddress(),
                stream.isClosed(),
                !stream.isClosedForRead(),
                !stream.isClosedForWrite());
        }
    }

}  // namespace sgns::ipfs_bitswap
