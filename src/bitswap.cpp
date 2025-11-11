#include "bitswap.hpp"

#include "bitswap_message.hpp"
#include <proto/unixfs.pb.h>
#include <proto/merkledag.pb.h>
#include "merkledag_encoder.hpp"

#include <string>
#include <tuple>
#include <fstream>
#include <filesystem>
#include <thread>
#include <algorithm>
#include <random>
#include <functional>
#include <sstream>
#include <iomanip>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include "merkledag_decoder.hpp"
#include <boost/asio/deadline_timer.hpp>

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
    case BitswapError::INVALID_UNIXFS_DATA:
        return "invalid UnixFS data format";
    case BitswapError::IPLD_DECODE_FAILURE:
        return "failed to decode IPLD node";
    case BitswapError::CONTENT_REQUEST_TIMEOUT:
        return "content request timeout";
    case BitswapError::FILE_NOT_FOUND:
        return "file not found";
    case BitswapError::ENCODING_FAILURE:
        return "failed to encode content";
    case BitswapError::BLOCK_NOT_FOUND:
        return "block not found in local store";
    }
    return "unknown bitswap error";
}

namespace
{
    const std::string bitswapProtocolId = "/ipfs/bitswap/1.0.0";
    
    // Helper function to safely convert CID to string
    std::string cidToString(const libp2p::multi::ContentIdentifier& cid) {
        auto result = libp2p::multi::ContentIdentifierCodec::toString(cid);
        return result.has_value() ? result.value() : "invalid";
    }

    // Helper function to calculate varint encoded length
    uint64_t getVarintEncodedLength(uint64_t value) {
        if (value < 0x80) return 1;
        if (value < 0x4000) return 2;
        if (value < 0x200000) return 3;
        if (value < 0x10000000) return 4;
        if (value < 0x800000000ULL) return 5;
        if (value < 0x40000000000ULL) return 6;
        if (value < 0x2000000000000ULL) return 7;
        if (value < 0x100000000000000ULL) return 8;
        if (value < 0x8000000000000000ULL) return 9;
        return 10;
    }
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

    ContentRequestContext::ContentRequestContext(boost::asio::io_context& context, const CID& rootCid)
        : rootCID(rootCid)
        , timeout(context)
        , contentTimeout_(boost::posix_time::seconds(30)) // 30 second timeout for complete content
    {
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

    void Bitswap::initialize()
    {
        // Set logger to debug level for detailed UnixFS analysis
        logger_->set_level(spdlog::level::debug);
        
        // Register this bitswap instance as the protocol handler for bitswap protocol
        host_.getRouter().setProtocolHandler(getProtocolId(), 
            [weak_self = std::weak_ptr<Bitswap>(shared_from_this())](auto stream_result) {
                if (auto self = weak_self.lock()) {
                    self->handle(stream_result);
                }
            });
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
                [ctx = shared_from_this(), stream, rw](libp2p::outcome::result<bitswap_pb::Message> rmsg) {
                    if (!rmsg)
                    {
                        ctx->logger_->error("bitswap message cannot be decoded");
                        return;
                    }

                    BitswapMessage msg(rmsg.value());
                    
                    ctx->logger_->debug("Received message: wantlist size: {}, blocks size: {}", 
                                       msg.GetWantlistSize(), msg.GetBlocksSize());

                    // If we receive a message with wantlist items, we act as a server
                    // If we receive a message with blocks, we act as a client
                    bool hasWantlist = msg.GetWantlistSize() > 0;
                    bool hasBlocks = msg.GetBlocksSize() > 0;
                    
                    ctx->logger_->debug("Message flags: hasWantlist={}, hasBlocks={}", hasWantlist, hasBlocks);
                    
                    // Process wantlist requests (server side)
                    for (int i = 0; i < msg.GetWantlistSize(); ++i)
                    {
                        auto blockId = msg.GetWantlistEntry(i).block();
                        auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)blockId.data(), blockId.size()));
                        if (cid) {
                            ctx->logger_->trace("wantlist item[{}]: {}", i, libp2p::multi::ContentIdentifierCodec::toString(cid.value()).value());
                            
                            // Check if we have this block and respond
                            ctx->handleWantlistRequest(cid.value(), stream);
                        }
                    }

                    ctx->logger_->debug("Processing {} blocks from bitswap message", msg.GetBlocksSize());
                    
                    // Process blocks (client side)
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
                            ctx->logger_->debug("Currently have {} request contexts", ctx->requestContexts_.size());
                            
                            auto itContext = ctx->requestContexts_.find(cid.value());
                            if (itContext != ctx->requestContexts_.end())
                            {
                                ctx->logger_->debug("Found matching request context for CID: {}, calling HandleResponse", scid);
                                
                                // Mark provider success
                                if (auto remotePeer = stream->remotePeerId()) {
                                    ctx->markProviderSuccess(cid.value(), remotePeer.value());
                                }
                                
                                itContext->second->HandleResponse(block);
                            }
                            else
                            {
                                ctx->logger_->warn("No request context found for received block CID: {}", scid);
                                // Debug: List all current request contexts
                                for (const auto& [reqCid, reqCtx] : ctx->requestContexts_) {
                                    auto reqCidStr = libp2p::multi::ContentIdentifierCodec::toString(reqCid);
                                    if (reqCidStr) {
                                        ctx->logger_->debug("  Available request context CID: {}", reqCidStr.value());
                                    }
                                }
                            }
                        }
                    }
                    
                    // Set up continuous reading based on what we received
                    if (hasWantlist && !hasBlocks) {
                        // We're a server that received a wantlist - set up continuous reading
                        ctx->logger_->debug("Server side: processed wantlist, setting up continuous read for subsequent requests");
                        
                        // Lambda function for recursive server reading
                        std::function<void()> setupServerRead = [ctx, stream, rw, setupServerRead = std::shared_ptr<std::function<void()>>(new std::function<void()>)]() mutable {
                            *setupServerRead = [ctx, stream, rw, setupServerRead]() {
                                rw->read<bitswap_pb::Message>(
                                    [ctx, stream, rw, setupServerRead](libp2p::outcome::result<bitswap_pb::Message> nextMsg) {
                                        if (nextMsg) {
                                            ctx->logger_->debug("Server received continuous request, processing...");
                                            BitswapMessage nextBitswapMsg(nextMsg.value());
                                            
                                            // Process the received message
                                            ctx->logger_->debug("Received message: wantlist size: {}, blocks size: {}", 
                                                               nextBitswapMsg.GetWantlistSize(), nextBitswapMsg.GetBlocksSize());
                                            
                                            bool nextHasWantlist = nextBitswapMsg.GetWantlistSize() > 0;
                                            bool nextHasBlocks = nextBitswapMsg.GetBlocksSize() > 0;
                                            ctx->logger_->debug("Message flags: hasWantlist={}, hasBlocks={}", nextHasWantlist, nextHasBlocks);
                                            
                                            // Process wantlist requests (server side)
                                            for (int i = 0; i < nextBitswapMsg.GetWantlistSize(); ++i) {
                                                auto blockId = nextBitswapMsg.GetWantlistEntry(i).block();
                                                auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)blockId.data(), blockId.size()));
                                                if (cid) {
                                                    ctx->logger_->trace("wantlist item[{}]: {}", i, libp2p::multi::ContentIdentifierCodec::toString(cid.value()).value());
                                                    ctx->handleWantlistRequest(cid.value(), stream);
                                                }
                                            }
                                            
                                            // Continue reading for more requests
                                            if (nextHasWantlist && !nextHasBlocks) {
                                                ctx->logger_->debug("Server side: processed wantlist, continuing continuous read loop");
                                                (*setupServerRead)();
                                            }
                                        } else {
                                            ctx->logger_->debug("Server continuous read ended: {}", nextMsg.error().message());
                                        }
                                    });
                            };
                            (*setupServerRead)();
                        };
                        
                        setupServerRead();
                    } else if (!hasWantlist && !hasBlocks) {
                        // We're a client that sent a wantlist and haven't received blocks yet
                        ctx->logger_->debug("Client side: setting up read for block response");
                        rw->read<bitswap_pb::Message>(
                            [ctx, stream, rw](libp2p::outcome::result<bitswap_pb::Message> nextMsg) {
                                ctx->logger_->debug("Client attempting to read response message...");
                                if (nextMsg) {
                                    BitswapMessage nextBitswapMsg(nextMsg.value());
                                    ctx->logger_->debug("Client received response with {} blocks", nextBitswapMsg.GetBlocksSize());
                                    
                                    // Process blocks in the response
                                    for (int blockIdx = 0; blockIdx < nextBitswapMsg.GetBlocksSize(); ++blockIdx) {
                                        const auto& block = nextBitswapMsg.GetBlock(blockIdx);
                                        ctx->logger_->debug("Response block received ({} bytes)", block.size());

                                        auto cidV0 = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(block.data(), block.size());
                                        auto cid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)cidV0.data(), cidV0.size()));
                                        if (cid) {
                                            auto scid = libp2p::multi::ContentIdentifierCodec::toString(cid.value()).value();
                                            ctx->logger_->debug("Response block CID: {}", scid);

                                            std::lock_guard<std::mutex> callbacksGuard(ctx->mutexRequestCallbacks_);
                                            auto itContext = ctx->requestContexts_.find(cid.value());
                                            if (itContext != ctx->requestContexts_.end()) {
                                                ctx->logger_->debug("Found matching request context for response CID: {}", scid);
                                                
                                                // Mark provider success
                                                if (auto remotePeer = stream->remotePeerId()) {
                                                    ctx->markProviderSuccess(cid.value(), remotePeer.value());
                                                }
                                                
                                                itContext->second->HandleResponse(block);
                                            }
                                        }
                                    }
                                } else {
                                    ctx->logger_->debug("Client read failed: {}", nextMsg.error().message());
                                }
                            });
                    } else {
                        ctx->logger_->debug("Message processed: wantlist={}, blocks={}", hasWantlist, hasBlocks);
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
            
            // Mark provider failure
            if (auto remotePeer = stream->remotePeerId()) {
                markProviderFailure(cid, remotePeer.value());
            }
            
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
        // Set up read operation to listen for server response
        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>(stream);
        logger_->debug("Client setting up read operation for server response to CID: {}", cidToString(cid));
        rw->read<bitswap_pb::Message>(
            [ctx = shared_from_this(), stream, cid](libp2p::outcome::result<bitswap_pb::Message> responseMsg) {
                ctx->logger_->debug("Client attempting to read response message for CID: {}", cidToString(cid));
                if (responseMsg) {
                    BitswapMessage responseBitswapMsg(responseMsg.value());
                    ctx->logger_->debug("Client received response with {} blocks for CID: {}", 
                                       responseBitswapMsg.GetBlocksSize(), cidToString(cid));
                    
                    // Process blocks in the response
                    for (int blockIdx = 0; blockIdx < responseBitswapMsg.GetBlocksSize(); ++blockIdx) {
                        const auto& block = responseBitswapMsg.GetBlock(blockIdx);
                        ctx->logger_->debug("Client response block received ({} bytes)", block.size());

                        auto cidV0 = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(block.data(), block.size());
                        auto blockCid = libp2p::multi::ContentIdentifierCodec::decode(gsl::span((uint8_t*)cidV0.data(), cidV0.size()));
                        if (blockCid) {
                            auto scid = libp2p::multi::ContentIdentifierCodec::toString(blockCid.value()).value();
                            ctx->logger_->debug("Client response block CID: {}", scid);

                            std::lock_guard<std::mutex> callbacksGuard(ctx->mutexRequestCallbacks_);
                            auto itContext = ctx->requestContexts_.find(blockCid.value());
                            if (itContext != ctx->requestContexts_.end()) {
                                ctx->logger_->debug("Client found matching request context for response CID: {}", scid);
                                
                                // Mark provider success
                                if (auto remotePeer = stream->remotePeerId()) {
                                    ctx->markProviderSuccess(blockCid.value(), remotePeer.value());
                                }
                                
                                itContext->second->HandleResponse(block);
                            } else {
                                ctx->logger_->warn("Client no request context found for received block CID: {}", scid);
                            }
                        }
                    }
                } else {
                    ctx->logger_->debug("Client read failed for CID {}: {}", cidToString(cid), responseMsg.error().message());
                }
            });

        // Keep stream open for reuse - don't close it
        logger_->debug("stream kept open for reuse");
    }

    void Bitswap::RequestContent(
        const libp2p::peer::PeerInfo& pi,
        const CID& cid,
        ContentCallback onContentCallback)
    {
        logger_->debug("RequestContent called for CID: {}", libp2p::multi::ContentIdentifierCodec::toString(cid).value());

        auto ctx = std::make_shared<ContentRequestContext>(*context_, cid);
        ctx->peerInfo = pi;  // Store peer info for additional requests
        ctx->callback = std::move(onContentCallback);
        ctx->pendingCIDs.insert(cid);

        {
            std::lock_guard<std::mutex> guard(mutexContentRequests_);
            contentRequests_[cid] = ctx;
        }

        // Set up timeout
        ctx->timeout.expires_from_now(ctx->contentTimeout_);
        ctx->timeout.async_wait([this, cid](const boost::system::error_code& ec) {
            if (!ec) {  // Timer wasn't cancelled
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                auto it = contentRequests_.find(cid);
                if (it != contentRequests_.end() && !it->second->timedOut) {
                    it->second->timedOut = true;
                    it->second->callback(BitswapError::CONTENT_REQUEST_TIMEOUT);
                    contentRequests_.erase(it);
                }
            }
        });

        // Start with the root CID
        RequestBlock(pi, cid, [this, ctx](libp2p::outcome::result<std::string> blockResult) {
            if (!blockResult) {
                if (!ctx->timedOut) {
                    ctx->callback(blockResult.error());
                    std::lock_guard<std::mutex> guard(mutexContentRequests_);
                    contentRequests_.erase(ctx->rootCID);
                }
                return;
            }
            processUnixFSBlock(ctx, ctx->rootCID, blockResult.value(), "");
        });
    }

    void Bitswap::RequestContent(
        const CID& cid,
        ContentCallback onContentCallback)
    {
        logger_->debug("RequestContent (auto-select peer) called for CID: {}", libp2p::multi::ContentIdentifierCodec::toString(cid).value());

        auto ctx = std::make_shared<ContentRequestContext>(*context_, cid);
        ctx->callback = std::move(onContentCallback);
        ctx->useProviders = true;  // Enable provider-based requests
        ctx->pendingCIDs.insert(cid);

        {
            std::lock_guard<std::mutex> guard(mutexContentRequests_);
            contentRequests_[cid] = ctx;
        }

        // Set up timeout
        ctx->timeout.expires_from_now(ctx->contentTimeout_);
        ctx->timeout.async_wait([this, cid](const boost::system::error_code& ec) {
            if (!ec) {  // Timer wasn't cancelled
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                auto it = contentRequests_.find(cid);
                if (it != contentRequests_.end() && !it->second->timedOut) {
                    it->second->timedOut = true;
                    it->second->callback(BitswapError::CONTENT_REQUEST_TIMEOUT);
                    contentRequests_.erase(it);
                }
            }
        });

        // Start with the root CID using provider-based requests
        try {
            auto selectedPeer = selectBestProvider(cid);
            ctx->peerInfo = selectedPeer;  // Store the selected peer for subsequent requests
            logger_->debug("Selected peer {} for content request CID: {}", 
                          selectedPeer.id.toBase58(), libp2p::multi::ContentIdentifierCodec::toString(cid).value());
            
            RequestBlock(selectedPeer, cid, [this, ctx](libp2p::outcome::result<std::string> blockResult) {
                if (!blockResult) {
                    if (!ctx->timedOut) {
                        ctx->callback(blockResult.error());
                        std::lock_guard<std::mutex> guard(mutexContentRequests_);
                        contentRequests_.erase(ctx->rootCID);
                    }
                    return;
                }
                processUnixFSBlock(ctx, ctx->rootCID, blockResult.value(), "");
            });
        } catch (const std::exception& e) {
            logger_->error("No providers available for root CID: {}", libp2p::multi::ContentIdentifierCodec::toString(cid).value());
            if (!ctx->timedOut) {
                ctx->callback(BitswapError::OUTBOUND_STREAM_FAILURE);
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                contentRequests_.erase(ctx->rootCID);
            }
        }
    }

    void Bitswap::RequestBlock(
        const libp2p::peer::PeerInfo& pi,
        const CID& cid,
        BlockCallback onBlockCallback)
    {
        RequestBlockWithRetry(pi, cid, std::move(onBlockCallback), 0);
    }

    void Bitswap::RequestBlockWithRetry(
        const libp2p::peer::PeerInfo& pi,
        const CID& cid,
        BlockCallback onBlockCallback,
        int retryCount)
    {
        const int maxRetries = 2;
        const int baseDelayMs = 500;
        
        // Check if connectable
        auto connectedness = host_.connectedness(pi);
        if (connectedness == libp2p::Host::Connectedness::CAN_NOT_CONNECT)
        {
            logger_->debug("Peer {} is not connectible", pi.id.toBase58());
            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
            return;
        }

        // First check if we have an active stream for this peer
        {
            std::lock_guard<std::mutex> guard(mutexActiveStreams_);
            auto streamIt = activeStreams_.find(pi.id);
            if (streamIt != activeStreams_.end() && !streamIt->second->isClosed()) {
                // Reuse existing stream
                logger_->debug("Reusing existing stream for peer {}", pi.id.toBase58());
                writeBitswapMessageToStream(streamIt->second, cid, std::move(onBlockCallback));
                return;
            } else if (streamIt != activeStreams_.end()) {
                // Remove closed stream from cache
                activeStreams_.erase(streamIt);
            }
        }

        // No active stream, create a new one
        if (retryCount > 0) {
            logger_->warn("Retrying stream creation for peer {} (attempt {}/{})", 
                         pi.id.toBase58(), retryCount + 1, maxRetries + 1);
        } else {
            logger_->debug("Creating new stream for peer {}", pi.id.toBase58());
        }
        
        host_.newStream(
            pi,
            bitswapProtocolId,
            [wp = weak_from_this(), cid(cid), pi(pi), onBlockCallback = std::move(onBlockCallback), retryCount, maxRetries, baseDelayMs]
                (libp2p::protocol::BaseProtocol::StreamResult rstream) mutable
            {
                auto ctx = wp.lock();
                if (ctx)
                {
                    if (!rstream)
                    {
                        ctx->logger_->error("Failed to create stream to peer {} (attempt {}): {}", 
                                           pi.id.toBase58(), retryCount + 1, rstream.error().message());
                        
                        // Mark provider failure
                        ctx->markProviderFailure(cid, pi.id);
                        
                        // Clear any stale cached streams for this peer
                        {
                            std::lock_guard<std::mutex> guard(ctx->mutexActiveStreams_);
                            ctx->activeStreams_.erase(pi.id);
                        }
                        
                        if (retryCount < maxRetries) {
                            // Retry with exponential backoff
                            int delay = baseDelayMs * (1 << retryCount); // 500ms, 1000ms, 2000ms
                            auto timer = std::make_shared<boost::asio::deadline_timer>(*ctx->context_);
                            timer->expires_from_now(boost::posix_time::milliseconds(delay));
                            timer->async_wait([ctx, pi, cid, onBlockCallback = std::move(onBlockCallback), retryCount, timer]
                                            (const boost::system::error_code& ec) mutable {
                                if (!ec) {
                                    ctx->RequestBlockWithRetry(pi, cid, std::move(onBlockCallback), retryCount + 1);
                                } else {
                                    onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
                                }
                            });
                        } else {
                            ctx->logger_->error("All retry attempts failed for peer {}", pi.id.toBase58());
                            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
                        }
                    }
                    else
                    {
                        auto stream = rstream.value();
                        if (retryCount > 0) {
                            ctx->logger_->info("Stream creation succeeded on retry {} for peer {}", 
                                             retryCount + 1, pi.id.toBase58());
                        }
                        ctx->logStreamState("outbound stream created", *stream);
                        
                        // Cache the stream for reuse
                        {
                            std::lock_guard<std::mutex> guard(ctx->mutexActiveStreams_);
                            ctx->activeStreams_[pi.id] = stream;
                        }
                        
                        ctx->writeBitswapMessageToStream(std::move(stream), cid, std::move(onBlockCallback));
                    }
                }
            },
            std::chrono::milliseconds(5000)); // Back to 5s timeout
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

    void Bitswap::processUnixFSBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const std::string& blockData, const std::string& path)
    {
        if (ctx->timedOut) {
            return; // Don't process if already timed out
        }

        logger_->set_level(spdlog::level::trace);
        logger_->debug("Processing UnixFS block for CID: {}", libp2p::multi::ContentIdentifierCodec::toString(cid).value());

        // Check if we've already processed this CID to prevent duplicates
        if (ctx->completedCIDs.find(cid) != ctx->completedCIDs.end()) {
            logger_->debug("CID already processed, skipping: {}", libp2p::multi::ContentIdentifierCodec::toString(cid).value());
            return;
        }

        // Mark this CID as completed
        ctx->pendingCIDs.erase(cid);
        ctx->completedCIDs.insert(cid);

        // Check if this CID is a chunk of a file in progress
        auto chunkIt = ctx->chunkToCidIndex.find(cid);
        if (chunkIt != ctx->chunkToCidIndex.end() && chunkIt->second.parentCid.has_value()) {
            // This is a file chunk
            const CID& parentCid = chunkIt->second.parentCid.value();
            size_t chunkIndex = chunkIt->second.chunkIndex;
            logger_->debug("Processing as chunk {} for file CID: {}", chunkIndex, 
                          libp2p::multi::ContentIdentifierCodec::toString(parentCid).value());
            handleFileChunk(ctx, cid, blockData, chunkIndex, parentCid);
            checkContentRequestComplete(ctx);
            return;
        }

        // First, decode the IPLD structure
        MerkledagDecoder decoder;
        bool diddecode = decoder.decode(blockData);
        
        if (!diddecode) {
            if (!ctx->timedOut) {
                ctx->callback(BitswapError::IPLD_DECODE_FAILURE);
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                contentRequests_.erase(ctx->rootCID);
            }
            return;
        }

        // ===== CID CALCULATION COMPARISON WITH KUBO =====
        // Test if our CID calculation method matches Kubo's by applying it to Kubo's raw IPLD data
        {
            std::vector<uint8_t> kuboBlockData(blockData.begin(), blockData.end());
            auto kuboCidBytes = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(kuboBlockData.data(), kuboBlockData.size());
            
            if (!kuboCidBytes.empty()) {
                auto kuboCidDecoded = libp2p::multi::ContentIdentifierCodec::decode(
                    gsl::span(reinterpret_cast<const uint8_t*>(kuboCidBytes.data()), kuboCidBytes.size())
                );
                if (kuboCidDecoded) {
                    auto kuboCidString = libp2p::multi::ContentIdentifierCodec::toString(kuboCidDecoded.value());
                    auto expectedCidString = libp2p::multi::ContentIdentifierCodec::toString(cid);
                    
                    logger_->debug("=== CID CALCULATION TEST ===");
                    logger_->debug("Expected CID: {}", expectedCidString.value());
                    logger_->debug("Kubo IPLD data -> our CID calc: {}", kuboCidString.value());
                    logger_->debug("CIDs match: {}", (kuboCidDecoded.value() == cid ? "YES" : "NO"));
                    logger_->debug("Kubo IPLD data size: {} bytes", kuboBlockData.size());
                    
                    // Log raw Kubo IPLD node bytes for direct comparison
                    std::ostringstream kuboHex;
                    for (size_t i = 0; i < std::min(kuboBlockData.size(), static_cast<size_t>(100)); ++i) {
                        kuboHex << std::hex << std::setfill('0') << std::setw(2) << (unsigned)kuboBlockData[i] << " ";
                    }
                    logger_->debug("Kubo IPLD node first 100 bytes (hex): {}", kuboHex.str());
                    logger_->debug("=== END CID TEST ===");
                } else {
                    logger_->debug("Failed to decode CID calculated from Kubo IPLD data");
                }
            } else {
                logger_->debug("Failed to calculate CID from Kubo IPLD data");
            }
        }

        // Process any links (additional CIDs to fetch)
        auto links = decoder.getLinks();
        for (const auto& link : links) {
            if (ctx->completedCIDs.find(link.cid) == ctx->completedCIDs.end()) {
                // We haven't processed this CID yet
                ctx->pendingCIDs.insert(link.cid);
                
                logger_->debug("Found linked CID: {}, queuing...", libp2p::multi::ContentIdentifierCodec::toString(link.cid).value());
                
                // Add to queue instead of making immediate request
                ctx->requestQueue.push(link.cid);
                processRequestQueue(ctx);
            }
        }

        // Parse UnixFS data from the IPLD content
        unixfs_pb::Data unixfsData;
        auto dataOpt = decoder.getData();
        if (!dataOpt || !unixfsData.ParseFromArray(dataOpt->data(), static_cast<int>(dataOpt->size()))) {
            if (!ctx->timedOut) {
                ctx->callback(BitswapError::INVALID_UNIXFS_DATA);
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                contentRequests_.erase(ctx->rootCID);
            }
            return;
        }
        logger_->set_level(spdlog::level::debug);
        // ===== DETAILED LOGGING FOR KUBO UNIXFS STRUCTURE ANALYSIS =====
        auto cidString = libp2p::multi::ContentIdentifierCodec::toString(cid);
        logger_->debug("==== KUBO UnixFS Analysis for CID: {} ====", cidString ? cidString.value() : "invalid");
        logger_->debug("UnixFS Type: {}", static_cast<int>(unixfsData.type()));
        logger_->debug("Has data field: {}", unixfsData.has_data());
        if (unixfsData.has_data()) {
            logger_->debug("Data field size: {} bytes", unixfsData.data().size());
        }
        logger_->debug("Has filesize field: {}", unixfsData.has_filesize());
        if (unixfsData.has_filesize()) {
            logger_->debug("Filesize: {} bytes", unixfsData.filesize());
        }
        logger_->debug("Has mode field: {}", unixfsData.has_mode());
        if (unixfsData.has_mode()) {
            logger_->debug("mode: {} ", unixfsData.mode());
        }
        logger_->debug("Has time field: {}", unixfsData.has_mtime());
        logger_->debug("Has fanout field: {}", unixfsData.has_fanout());
        logger_->debug("Has type field: {}", unixfsData.has_type());
        // if (unixfsData.has_mtime()) {
        //     logger_->debug("mtime: {} ", unixfsData.mtime());
        // }
        logger_->debug("Blocksizes count: {}", unixfsData.blocksizes_size());
        for (int i = 0; i < unixfsData.blocksizes_size(); ++i) {
            logger_->debug("  Blocksize[{}]: {} bytes", i, unixfsData.blocksizes(i));
        }
        logger_->debug("IPLD Links count: {}", links.size());
        for (size_t i = 0; i < links.size(); ++i) {
            const auto& link = links[i];
            auto subcidString = libp2p::multi::ContentIdentifierCodec::toString(link.cid);
            std::string cidStr = subcidString ? subcidString.value() : "invalid";
            logger_->debug("  Link[{}]: name='{}' -> CID='{}' size='{}'", i, link.name, cidStr, link.size);
            
            // Log raw link CID bytes for comparison
            auto cidBytes = libp2p::multi::ContentIdentifierCodec::encode(link.cid);
            if (cidBytes.has_value()) {
                std::ostringstream linkHex;
                for (size_t j = 0; j < cidBytes.value().size(); ++j) {
                    linkHex << std::hex << std::setfill('0') << std::setw(2) << (unsigned)cidBytes.value()[j] << " ";
                }
                logger_->debug("    Kubo Link[{}] CID bytes (hex): {}", i, linkHex.str());
            }
        }
        
        // Log raw UnixFS protobuf data for comparison
        size_t serializedSize = unixfsData.ByteSizeLong();
        std::vector<uint8_t> serializedUnixFS(serializedSize);
        unixfsData.SerializeToArray(serializedUnixFS.data(), static_cast<int>(serializedSize));
        std::ostringstream hexStream;
        for (unsigned char c : serializedUnixFS) {
            hexStream << std::hex << std::setfill('0') << std::setw(2) << (unsigned)c << " ";
        }
        logger_->debug("Raw UnixFS protobuf (hex): {}", hexStream.str());
        logger_->debug("==== End Kubo UnixFS Analysis ====");

        // Handle different UnixFS data types
        switch (unixfsData.type()) {
            case unixfs_pb::Data::Raw:
            case unixfs_pb::Data::File:
                handleFileBlock(ctx, cid, unixfsData, decoder, path);
                break;
                
            case unixfs_pb::Data::Directory:
                handleDirectoryBlock(ctx, cid, unixfsData, decoder, path);
                break;
                
            default:
                //logger_->warn("Unsupported UnixFS data type: {}", unixfsData.type());
                // For now, treat as raw data
                handleFileBlock(ctx, cid, unixfsData, decoder, path);
                break;
        }

        checkContentRequestComplete(ctx);
    }

    void Bitswap::handleFileBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const unixfs_pb::Data& unixfsData, const MerkledagDecoder& decoder, const std::string& path)
    {
        auto links = decoder.getLinks();
        
        // Use the path parameter, but if it's empty, try to look up from cidToPath
        std::string filePath = path;
        if (filePath.empty()) {
            auto pathIt = ctx->cidToPath.find(cid);
            if (pathIt != ctx->cidToPath.end()) {
                filePath = pathIt->second;
                logger_->debug("Found path for CID in cidToPath: {}", filePath);
            }
        }
        
        // If there are no links, this is a complete file block or single chunk
        if (links.size() == 0) {
            if (unixfsData.has_data()) {
                UnixFSFile file;
                file.path = filePath;
                file.content = std::vector<char>(unixfsData.data().begin(), unixfsData.data().end());
                file.size = unixfsData.has_filesize() ? unixfsData.filesize() : unixfsData.data().size();
                
                if (unixfsData.has_mode()) {
                    file.mode = unixfsData.mode();
                }
                if (unixfsData.has_mtime()) {
                    file.mtime = unixfsData.mtime();
                }

                ctx->collectedFiles.push_back(std::move(file));
                logger_->debug("Complete file collected: {} ({} bytes)", filePath, file.size);
            }
        } else {
            // This file has multiple chunks - set up for chunk assembly
            auto links = decoder.getLinks();
            logger_->debug("Multi-chunk file detected: {} with {} chunks", filePath, links.size());
            
            // Check if we already have this file in progress to prevent duplicates
            if (ctx->filesInProgress.find(cid) != ctx->filesInProgress.end()) {
                logger_->debug("File already in progress, skipping duplicate setup: {}", filePath);
                return;
            }
            
            ContentRequestContext::FileInProgress& fileProgress = ctx->filesInProgress[cid];
            fileProgress.path = filePath;
            fileProgress.expectedChunks = links.size();
            fileProgress.totalSize = unixfsData.has_filesize() ? unixfsData.filesize() : 0;
            
            if (unixfsData.has_mode()) {
                fileProgress.mode = unixfsData.mode();
            }
            if (unixfsData.has_mtime()) {
                fileProgress.mtime = unixfsData.mtime();
            }
            
            // If there's immediate data in the root block, store it as chunk 0
            if (unixfsData.has_data()) {
                ContentRequestContext::FileChunk chunk{
                    std::vector<char>(unixfsData.data().begin(), unixfsData.data().end()),
                    0,
                    std::make_optional(cid)
                };
                fileProgress.chunks[0] = std::move(chunk);
                logger_->debug("Stored root chunk 0 ({} bytes)", chunk.data.size());
            }
            
            // Request all linked chunks
            for (size_t i = 0; i < links.size(); ++i) {
                const auto& link = links[i];
                
                if (ctx->completedCIDs.find(link.cid) == ctx->completedCIDs.end()) {
                    ctx->pendingCIDs.insert(link.cid);
                    ctx->cidToPath[link.cid] = filePath; // Track which file this chunk belongs to
                    ctx->chunkToCidIndex[link.cid] = ContentRequestContext::ChunkInfo(cid, i); // Track this is chunk i of file cid
                    
                    logger_->debug("Requesting file chunk {} for {}", i + 1, filePath);
                    
                    // Add to queue instead of making immediate request
                    ctx->requestQueue.push(link.cid);
                    processRequestQueue(ctx);
                }
            }
        }
    }

    void Bitswap::handleFileChunk(std::shared_ptr<ContentRequestContext> ctx, const CID& chunkCid, const std::string& chunkData, size_t chunkIndex, const CID& parentCid)
    {
        ctx->completedCIDs.insert(chunkCid);
        ctx->pendingCIDs.erase(chunkCid);
        
        // Find the file this chunk belongs to
        auto fileIt = ctx->filesInProgress.find(parentCid);
        if (fileIt == ctx->filesInProgress.end()) {
            logger_->error("Received chunk for unknown file CID: {}", libp2p::multi::ContentIdentifierCodec::toString(parentCid).value());
            // Try to recreate the file progress entry if we can find the path
            auto pathIt = ctx->cidToPath.find(parentCid);
            if (pathIt != ctx->cidToPath.end()) {
                logger_->debug("Attempting to recreate file progress entry for: {}", pathIt->second);
                // Create a minimal file progress entry
                ContentRequestContext::FileInProgress& fileProgress = ctx->filesInProgress[parentCid];
                fileProgress.path = pathIt->second;
                fileProgress.expectedChunks = 1; // We'll adjust this as we get more chunks
            } else {
                logger_->debug("Assembled empty directory");
                return;
            }
        }
        
        auto& fileProgress = ctx->filesInProgress[parentCid];
        
        // Decode the chunk data to extract the actual content
        MerkledagDecoder decoder;
        auto didDecode = decoder.decode(chunkData);
        
        if (!didDecode) {
            logger_->error("Failed to decode IPLD chunk for file: {}", fileProgress.path);
            return;
        }
        
        // For file chunks, we typically want the raw content
        std::vector<char> chunkContent;
        
        // Try to parse as UnixFS data first
        unixfs_pb::Data unixfsData;
        auto dataOpt = decoder.getData();
        if (dataOpt && unixfsData.ParseFromArray(dataOpt->data(), static_cast<int>(dataOpt->size())) && unixfsData.has_data()) {
            // This chunk contains UnixFS wrapped data
            chunkContent = std::vector<char>(unixfsData.data().begin(), unixfsData.data().end());
            logger_->debug("Decoded UnixFS chunk {} for {} ({} bytes)", chunkIndex, fileProgress.path, chunkContent.size());
            
            // For first chunk, log some raw data comparison
            if (chunkIndex == 0) {
                std::stringstream hex_stream;
                for (size_t i = 0; i < std::min(chunkContent.size(), static_cast<size_t>(32)); ++i) {
                    hex_stream << std::hex << std::setfill('0') << std::setw(2) 
                              << static_cast<unsigned>(static_cast<unsigned char>(chunkContent[i])) << " ";
                }
                logger_->debug("Kubo chunk 0 first 32 bytes (hex): {}", hex_stream.str());
            }
            logger_->debug("Chunk {} UnixFS filesize field present: {}", chunkIndex, unixfsData.has_filesize());
            logger_->debug("Chunk {} UnixFS mode field present: {}", chunkIndex, unixfsData.has_mode());
            logger_->debug("Chunk {} UnixFS mtime field present: {}", chunkIndex, unixfsData.has_mtime());
            logger_->debug("Chunk {} UnixFS type field present: {}", chunkIndex, unixfsData.has_type());
        } else {
            // This might be raw data
            if (dataOpt) {
                chunkContent = std::vector<char>(dataOpt->begin(), dataOpt->end());
            }
            logger_->debug("Decoded raw chunk {} for {} ({} bytes)", chunkIndex, fileProgress.path, chunkContent.size());
        }
        
        // Store the chunk
        ContentRequestContext::FileChunk chunk{
            std::move(chunkContent),
            chunkIndex,
            std::make_optional(chunkCid)
        };
        fileProgress.chunks[chunkIndex] = std::move(chunk);
        
        // Update expected chunks if we got a higher index
        if (chunkIndex + 1 > fileProgress.expectedChunks) {
            fileProgress.expectedChunks = chunkIndex + 1;
        }
        
        logger_->debug("Stored chunk {} for file {} ({}/{} chunks)", 
                      chunkIndex, fileProgress.path, fileProgress.chunks.size(), fileProgress.expectedChunks);
        
        // Check if we have all chunks for this file
        if (fileProgress.chunks.size() >= fileProgress.expectedChunks) {
            // Assemble the complete file
            assembleCompleteFile(ctx, parentCid, fileProgress);
        }
    }

    void Bitswap::assembleCompleteFile(std::shared_ptr<ContentRequestContext> ctx, const CID& fileCid, const ContentRequestContext::FileInProgress& fileProgress)
    {
        // Create the complete file by concatenating chunks in order
        UnixFSFile completeFile;
        completeFile.path = fileProgress.path;
        completeFile.mode = fileProgress.mode;
        completeFile.mtime = fileProgress.mtime;
        
        // Calculate total size and reserve space
        size_t totalContentSize = 0;
        for (const auto& [index, chunk] : fileProgress.chunks) {
            totalContentSize += chunk.data.size();
        }
        
        completeFile.content.reserve(totalContentSize);
        completeFile.size = fileProgress.totalSize > 0 ? fileProgress.totalSize : totalContentSize;
        
        // Concatenate chunks in order
        size_t missingChunks = 0;
        for (size_t i = 0; i < fileProgress.expectedChunks; ++i) {
            auto chunkIt = fileProgress.chunks.find(i);
            if (chunkIt != fileProgress.chunks.end()) {
                const auto& chunkData = chunkIt->second.data;
                completeFile.content.insert(completeFile.content.end(), chunkData.begin(), chunkData.end());
            } else {
                logger_->warn("Missing chunk {} for file {}", i, fileProgress.path);
                missingChunks++;
            }
        }
        
        if (missingChunks > 0) {
            logger_->warn("File {} assembled with {} missing chunks out of {}", 
                         fileProgress.path, missingChunks, fileProgress.expectedChunks);
        }
        
        logger_->info("Assembled complete file: {} ({} bytes from {}/{} chunks)", 
                     completeFile.path, completeFile.content.size(), 
                     fileProgress.chunks.size(), fileProgress.expectedChunks);
        
        // Add to collected files
        ctx->collectedFiles.push_back(std::move(completeFile));
        
        // Remove from files in progress
        ctx->filesInProgress.erase(fileCid);
        
        // Clear chunk mappings for this file
        for (auto it = ctx->chunkToCidIndex.begin(); it != ctx->chunkToCidIndex.end();) {
            if (it->second.parentCid == fileCid) {
                it = ctx->chunkToCidIndex.erase(it);
            } else {
                ++it;
            }
        }
    }

    void Bitswap::handleDirectoryBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const unixfs_pb::Data& unixfsData, const MerkledagDecoder& decoder, const std::string& basePath)
    {
        auto links = decoder.getLinks();
        logger_->debug("Processing directory block with {} entries, basePath: '{}'", links.size(), basePath);
        
        // Directory blocks contain links to their contents
        for (const auto& link : links) {
            if (link.name.empty()) {
                logger_->warn("Directory entry has empty name, skipping");
                continue;
            }
            
            // Construct the full path for this entry
            std::string childPath = basePath.empty() ? link.name : basePath + "/" + link.name;
            
            // Check if we've already processed this CID
            if (ctx->completedCIDs.find(link.cid) != ctx->completedCIDs.end()) {
                logger_->debug("Already processed CID for {}, skipping", childPath);
                continue;
            }
            
            // Always ensure the path mapping is set up, even for duplicates
            ctx->cidToPath[link.cid] = childPath;
            
            // Check if we're already processing this CID
            if (ctx->pendingCIDs.find(link.cid) != ctx->pendingCIDs.end()) {
                logger_->debug("Already pending CID for {}, skipping duplicate", childPath);
                continue;
            }
            
            // Add to pending
            ctx->pendingCIDs.insert(link.cid);
            
            logger_->debug("Requesting directory entry: {} -> {}", childPath, 
                          libp2p::multi::ContentIdentifierCodec::toString(link.cid).value());
            
            // Add to request queue instead of making direct request
            ctx->requestQueue.push(link.cid);
        }
        
        logger_->debug("Directory block processed: {} entries queued for processing", links.size());
        
        // Process the queued directory entries
        processRequestQueue(ctx);
    }

    void Bitswap::checkContentRequestComplete(std::shared_ptr<ContentRequestContext> ctx)
    {
        if (ctx->timedOut) {
            return;
        }

        // Check if all requests are complete (no pending CIDs and no files in progress)
        if (ctx->pendingCIDs.empty() && ctx->filesInProgress.empty()) {
            // All blocks received - assemble final content
            UnixFSContent content = assembleContent(ctx);
            ctx->timeout.cancel(); // Cancel the timeout timer
            ctx->callback(std::move(content));
            
            std::lock_guard<std::mutex> guard(mutexContentRequests_);
            contentRequests_.erase(ctx->rootCID);
        } else {
            logger_->debug("Content request not complete: {} pending CIDs, {} files in progress", 
                          ctx->pendingCIDs.size(), ctx->filesInProgress.size());
        }
    }

    UnixFSContent Bitswap::assembleContent(std::shared_ptr<ContentRequestContext> ctx)
    {
        UnixFSContent content;
        
        // Determine content type based on collected files
        if (ctx->collectedFiles.empty()) {
            content.type = UnixFSContent::DIRECTORY;
            logger_->debug("Assembled empty directory");
        } else if (ctx->collectedFiles.size() == 1 && ctx->collectedFiles[0].path.empty()) {
            content.type = UnixFSContent::SINGLE_FILE;
            logger_->debug("Assembled single file: {} bytes", ctx->collectedFiles[0].content.size());
        } else {
            // Multiple files or files with paths - this is a directory structure
            bool hasDirectoryStructure = false;
            for (const auto& file : ctx->collectedFiles) {
                if (!file.path.empty() && file.path.find('/') != std::string::npos) {
                    hasDirectoryStructure = true;
                    break;
                }
            }
            
            content.type = hasDirectoryStructure ? UnixFSContent::MULTI_FILE_ARCHIVE : UnixFSContent::DIRECTORY;
            logger_->debug("Assembled {} with {} files", 
                          (content.type == UnixFSContent::MULTI_FILE_ARCHIVE ? "multi-file archive" : "directory"),
                          ctx->collectedFiles.size());
        }
        
        // Move files to content
        content.files = std::move(ctx->collectedFiles);
        
        // Add metadata about the request
        content.metadata["root_cid"] = libp2p::multi::ContentIdentifierCodec::toString(ctx->rootCID).value();
        content.metadata["total_files"] = std::to_string(content.files.size());
        
        size_t totalSize = 0;
        for (const auto& file : content.files) {
            totalSize += file.content.size();
        }
        content.metadata["total_size"] = std::to_string(totalSize);
        
        return content;
    }

    void Bitswap::processRequestQueue(std::shared_ptr<ContentRequestContext> ctx)
    {
        if (ctx->processingQueue || ctx->requestQueue.empty() || ctx->timedOut) {
            return;
        }
        
        ctx->processingQueue = true;
        
        // Process one request at a time
        auto nextCid = ctx->requestQueue.front();
        ctx->requestQueue.pop();
        
        logger_->debug("Processing queued request for CID: {}", libp2p::multi::ContentIdentifierCodec::toString(nextCid).value());
        
        // Always try to use the same peer first if available, then fall back to providers
        if (ctx->peerInfo.has_value()) {
            logger_->debug("Using same peer for queued CID: {} from peer: {}", 
                          libp2p::multi::ContentIdentifierCodec::toString(nextCid).value(),
                          ctx->peerInfo->id.toBase58());
            
            RequestBlock(ctx->peerInfo.value(), nextCid, [this, ctx, nextCid](libp2p::outcome::result<std::string> result) {
                ctx->processingQueue = false;
                
                if (!result) {
                    // If same-peer request failed and we're using providers, try provider system as fallback
                    if (ctx->useProviders) {
                        logger_->debug("Same-peer request failed for CID: {}, trying provider system as fallback using root CID providers", 
                                      libp2p::multi::ContentIdentifierCodec::toString(nextCid).value());
                        
                        requestBlockWithProvidersFromRoot(ctx->rootCID, nextCid, [this, ctx, nextCid](libp2p::outcome::result<std::string> fallbackResult) {
                            if (!fallbackResult) {
                                if (!ctx->timedOut) {
                                    ctx->callback(fallbackResult.error());
                                    std::lock_guard<std::mutex> guard(mutexContentRequests_);
                                    contentRequests_.erase(ctx->rootCID);
                                }
                                return;
                            }
                            
                            // Look up the path for this CID
                            std::string path = "";
                            auto pathIt = ctx->cidToPath.find(nextCid);
                            if (pathIt != ctx->cidToPath.end()) {
                                path = pathIt->second;
                            }
                            
                            processUnixFSBlock(ctx, nextCid, fallbackResult.value(), path);
                            
                            // Process next item in queue after a short delay
                            auto timer = std::make_shared<boost::asio::deadline_timer>(*context_);
                            timer->expires_from_now(boost::posix_time::milliseconds(200));
                            timer->async_wait([this, ctx, timer](const boost::system::error_code& ec) {
                                if (!ec && !ctx->timedOut) {
                                    processRequestQueue(ctx);
                                }
                            });
                        });
                        return;
                    }
                    
                    if (!ctx->timedOut) {
                        ctx->callback(result.error());
                        std::lock_guard<std::mutex> guard(mutexContentRequests_);
                        contentRequests_.erase(ctx->rootCID);
                    }
                    return;
                }
                
                // Look up the path for this CID
                std::string path = "";
                auto pathIt = ctx->cidToPath.find(nextCid);
                if (pathIt != ctx->cidToPath.end()) {
                    path = pathIt->second;
                }
                
                processUnixFSBlock(ctx, nextCid, result.value(), path);
                
                // Process next item in queue after a short delay
                auto timer = std::make_shared<boost::asio::deadline_timer>(*context_);
                timer->expires_from_now(boost::posix_time::milliseconds(200));
                timer->async_wait([this, ctx, timer](const boost::system::error_code& ec) {
                    if (!ec && !ctx->timedOut) {
                        processRequestQueue(ctx);
                    }
                });
            });
        } else if (ctx->useProviders) {
            // Pure provider-based request (no fallback peer available)
            requestBlockWithProviders(nextCid, [this, ctx, nextCid](libp2p::outcome::result<std::string> result) {
                ctx->processingQueue = false;
                
                if (!result) {
                    if (!ctx->timedOut) {
                        ctx->callback(result.error());
                        std::lock_guard<std::mutex> guard(mutexContentRequests_);
                        contentRequests_.erase(ctx->rootCID);
                    }
                    return;
                }
                
                // Look up the path for this CID
                std::string path = "";
                auto pathIt = ctx->cidToPath.find(nextCid);
                if (pathIt != ctx->cidToPath.end()) {
                    path = pathIt->second;
                }
                
                processUnixFSBlock(ctx, nextCid, result.value(), path);
                
                // Process next item in queue after a short delay
                auto timer = std::make_shared<boost::asio::deadline_timer>(*context_);
                timer->expires_from_now(boost::posix_time::milliseconds(200));
                timer->async_wait([this, ctx, timer](const boost::system::error_code& ec) {
                    if (!ec && !ctx->timedOut) {
                        processRequestQueue(ctx);
                    }
                });
            });
        } else {
            // Legacy behavior - should not reach here in normal operation
            logger_->error("No peer info and providers disabled for CID: {}", 
                          libp2p::multi::ContentIdentifierCodec::toString(nextCid).value());
            ctx->processingQueue = false;
            if (!ctx->timedOut) {
                ctx->callback(BitswapError::OUTBOUND_STREAM_FAILURE);
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                contentRequests_.erase(ctx->rootCID);
            }
        }
    }

    // Server-side implementation methods

    CID Bitswap::encodeAndStoreFile(const std::string& filePath)
    {
        namespace fs = std::filesystem;
        
        if (!fs::exists(filePath) || !fs::is_regular_file(filePath)) {
            logger_->error("File not found or not a regular file: {}", filePath);
            throw std::runtime_error("File not found: " + filePath);
        }

        std::ifstream file(filePath, std::ios::binary);
        if (!file) {
            logger_->error("Failed to open file: {}", filePath);
            throw std::runtime_error("Failed to open file: " + filePath);
        }

        // Read file content
        file.seekg(0, std::ios::end);
        size_t fileSize = file.tellg();
        file.seekg(0, std::ios::beg);
        
        std::vector<uint8_t> content(fileSize);
        file.read(reinterpret_cast<char*>(content.data()), fileSize);
        file.close();

        logger_->debug("Read file: {} ({} bytes)", filePath, fileSize);

        // For large files, we should implement chunking
        // For now, handle files up to a reasonable size as single blocks
        const size_t CHUNK_SIZE = 256 * 1024; // 256KB chunks
        
        if (fileSize <= CHUNK_SIZE) {
            // Single block file
            return encodeAndStoreData(content, unixfs_pb::Data::File);
        } else {
            // Multi-chunk file - need to implement chunking
            return encodeChunkedFile(content, filePath);
        }
    }

    std::string Bitswap::bytesToHex(const std::vector<uint8_t>& bytes)
    {
        std::ostringstream hexStream;
        for (uint8_t byte : bytes) {
            hexStream << std::hex << std::setfill('0') << std::setw(2) << static_cast<unsigned>(byte) << " ";
        }
        return hexStream.str();
    }

    void Bitswap::analyzeIPLDStructure(const std::vector<uint8_t>& data, const std::string& label)
    {
        logger_->debug("=== IPLD Structure Analysis: {} ===", label);
        logger_->debug("Total size: {} bytes", data.size());
        
        // Analyze protobuf structure
        if (data.size() > 10) {
            for (size_t i = 0; i < std::min(size_t(20), data.size()); ++i) {
                uint8_t byte = data[i];
                uint8_t tag = (byte >> 3) & 0x1F;
                uint8_t wireType = byte & 0x07;
                
                if (wireType <= 5) { // Valid wire types
                    logger_->debug("Byte {}: 0x{:02X} (tag={}, wire_type={})", i, byte, tag, wireType);
                }
            }
        }
        logger_->debug("=== End Analysis ===");
    }

    CID Bitswap::encodeChunkedFile(const std::vector<uint8_t>& content, const std::string& filePath)
    {
        const size_t CHUNK_SIZE = 256 * 1024; // 256KB chunks
        std::vector<CID> chunkCIDs;
        std::vector<uint64_t> chunkSizes; // This will store the actual IPLD block sizes (including UnixFS wrapper)
        
        // Create chunks - chunks are stored as Raw blocks with UnixFS wrapper
        for (size_t offset = 0; offset < content.size(); offset += CHUNK_SIZE) {
            size_t rawChunkSize = std::min(CHUNK_SIZE, content.size() - offset);
            std::vector<uint8_t> chunk(content.begin() + offset, content.begin() + offset + rawChunkSize);
            
            // For first chunk, log some raw data comparison
            if (offset == 0) {
                std::stringstream hex_stream;
                for (size_t i = 0; i < std::min(chunk.size(), static_cast<size_t>(32)); ++i) {
                    hex_stream << std::hex << std::setfill('0') << std::setw(2) 
                              << static_cast<unsigned>(chunk[i]) << " ";
                }
                logger_->debug("Our chunk 0 first 32 bytes (hex): {}", hex_stream.str());
            }
            
            // Store chunk with UnixFS wrapper (File type to match Kubo)
            CID chunkCID = createIPLDNodeAndStoreRawData(chunk);
            if (chunkCID.content_address.toBuffer().empty()) {
                logger_->error("Failed to encode chunk for file: {}", filePath);
                throw std::runtime_error("Failed to encode chunk for file: " + filePath);
            }
            
            // Use raw chunk size for blocksizes field (as confirmed by Kubo analysis)
            chunkSizes.push_back(chunk.size());
            chunkCIDs.push_back(chunkCID);
        }

        // Create root UnixFS node that links to all chunks
        unixfs_pb::Data unixfsData;
        unixfsData.set_type(unixfs_pb::Data::File);
        unixfsData.set_filesize(content.size());
        // Add blocksizes for each chunk in order
        for (uint64_t size : chunkSizes) {
            unixfsData.add_blocksizes(size);
        }
        
        // Do NOT set data field for chunked files (only blocksizes and links)

        // Serialize UnixFS data using SerializeToArray for consistency with ParseFromArray
        // size_t serializedSize = unixfsData.ByteSizeLong();
        // std::vector<uint8_t> serializedUnixFS(serializedSize);
        std::string serializedUnixFS;
        if (!unixfsData.SerializeToString(&serializedUnixFS)) {
            logger_->error("Failed to serialize UnixFS data for chunked file: {}", filePath);
            throw std::runtime_error("Failed to serialize UnixFS data for chunked file: " + filePath);
        }

        // ===== DETAILED LOGGING FOR CHUNKED FILE ROOT =====
        logger_->debug("==== Our Chunked File Root UnixFS ====");
        logger_->debug("File: {}", filePath);
        logger_->debug("Total file size: {} bytes", content.size());
        logger_->debug("Number of chunks: {}", chunkCIDs.size());
        logger_->debug("UnixFS Type: {}", static_cast<int>(unixfsData.type()));
        logger_->debug("Has data field: {} (should be false for chunked files)", unixfsData.has_data());
        logger_->debug("Has filesize field: {}", unixfsData.has_filesize());
        if (unixfsData.has_filesize()) {
            logger_->debug("Filesize: {} bytes", unixfsData.filesize());
        }
        logger_->debug("Blocksizes count: {}", unixfsData.blocksizes_size());
        for (int i = 0; i < unixfsData.blocksizes_size(); ++i) {
            logger_->debug("  Blocksize[{}]: {} bytes", i, unixfsData.blocksizes(i));
        }
        
        // Log chunk CIDs
        for (size_t i = 0; i < chunkCIDs.size(); ++i) {
            auto chunkCidString = libp2p::multi::ContentIdentifierCodec::toString(chunkCIDs[i]);
            logger_->debug("  Chunk[{}] CID: {}", i, chunkCidString ? chunkCidString.value() : "invalid");
        }
        
        // Log raw UnixFS protobuf data for root node
        std::ostringstream hexStream;
        for (unsigned char c : serializedUnixFS) {
            hexStream << std::hex << std::setfill('0') << std::setw(2) << (unsigned)c << " ";
        }
        logger_->debug("Chunked file root UnixFS protobuf (hex): {}", hexStream.str());
        logger_->debug("==== End Chunked File Root UnixFS ====");

        // Create root IPLD node with ordered chunk CIDs (preserves order and allows duplicate empty names)
        CID rootCID = createIPLDNode(serializedUnixFS, chunkCIDs);
        
        logger_->debug("Created chunked file with {} chunks, root CID: {}", 
                      chunkCIDs.size(), 
                      libp2p::multi::ContentIdentifierCodec::toString(rootCID).has_value() 
                          ? libp2p::multi::ContentIdentifierCodec::toString(rootCID).value() 
                          : "invalid");
        
        return rootCID;
    }

    CID Bitswap::encodeAndStoreDirectory(const std::string& directoryPath)
    {
        namespace fs = std::filesystem;
        
        if (!fs::exists(directoryPath) || !fs::is_directory(directoryPath)) {
            logger_->error("Directory not found: {}", directoryPath);
            throw std::runtime_error("Directory not found: " + directoryPath);
        }

        // Create directory UnixFS data
        unixfs_pb::Data unixfsData;
        unixfsData.set_type(unixfs_pb::Data::Directory);

        std::map<std::string, CID> links;
        
        // Process directory entries
        for (const auto& entry : fs::directory_iterator(directoryPath)) {
            std::string entryName = entry.path().filename().string();
            
            try {
                CID entryCID = [&]() -> CID {
                    if (entry.is_regular_file()) {
                        return encodeAndStoreFile(entry.path().string());
                    } else if (entry.is_directory()) {
                        return encodeAndStoreDirectory(entry.path().string());
                    } else {
                        logger_->warn("Skipping unsupported file type: {}", entry.path().string());
                        throw std::runtime_error("Unsupported file type");
                    }
                }();
                
                if (!entryCID.content_address.toBuffer().empty()) {
                    links.emplace(entryName, entryCID);
                    logger_->debug("Added directory entry: {} -> {}", entryName, cidToString(entryCID));
                }
            } catch (const std::exception& e) {
                logger_->warn("Failed to process directory entry {}: {}", entry.path().string(), e.what());
                // Continue with other entries
            }
        }

        // Serialize UnixFS data using SerializeToArray for consistency with ParseFromArray
        // size_t serializedSize = unixfsData.ByteSizeLong();
        // std::vector<uint8_t> serializedUnixFS(serializedSize);
        std::string serializedUnixFS;
        if (!unixfsData.SerializeToString(&serializedUnixFS)) {
            logger_->error("Failed to serialize UnixFS data for directory: {}", directoryPath);
            throw std::runtime_error("Failed to serialize UnixFS data for directory: " + directoryPath);
        }

        // Create IPLD node
        CID dirCID = createIPLDNode(serializedUnixFS, links);
        
        logger_->debug("Created directory with {} entries, CID: {}", 
                      links.size(), cidToString(dirCID));
        
        return dirCID;
    }

    CID Bitswap::encodeAndStoreData(const std::vector<uint8_t>& data, unixfs_pb::Data::DataType type)
    {
        // Create UnixFS data
        std::string unixfsData = createUnixFSData(data, type, data.size());
        
        // Create IPLD node to calculate the CID, but store the UnixFS data for bitswap
        CID cid = createIPLDNodeAndStoreUnixFS(unixfsData);
        
        if (!cid.content_address.toBuffer().empty()) {
            logger_->debug("Encoded and stored {} bytes as CID: {}", data.size(), cidToString(cid));
        }
        
        return cid;
    }

    std::string Bitswap::createUnixFSData(const std::vector<uint8_t>& content, 
                                                   unixfs_pb::Data::DataType type, 
                                                   uint64_t filesize, 
                                                   const std::vector<CID>& links)
    {
        unixfs_pb::Data unixfsData;
        unixfsData.set_type(type);
        
        if (!content.empty()) {
            unixfsData.set_data(content.data(), content.size());
        }
        
        if (filesize > 0 && (type == unixfs_pb::Data::File || type == unixfs_pb::Data::Raw)) {
            unixfsData.set_filesize(filesize);
        }
        
        // Add block sizes for linked chunks
        for (const auto& link : links) {
            // In a real implementation, we'd look up the actual size
            // For now, we'll handle this in the chunked file method
        }

        // Serialize UnixFS data using SerializeToArray for consistency with ParseFromArray
        //size_t serializedSize = unixfsData.ByteSizeLong();
        std::string serialized;
        if (!unixfsData.SerializeToString(&serialized)) {
            logger_->error("Failed to serialize UnixFS data");
            return {};
        }

        // ===== DETAILED LOGGING FOR OUR UNIXFS CREATION =====
        logger_->debug("==== Our UnixFS Creation ====");
        logger_->debug("Creating UnixFS Type: {}", static_cast<int>(unixfsData.type()));
        logger_->debug("Has data field: {}", unixfsData.has_data());
        if (unixfsData.has_data()) {
            logger_->debug("Data field size: {} bytes", unixfsData.data().size());
        }
        logger_->debug("Has filesize field: {}", unixfsData.has_filesize());
        if (unixfsData.has_filesize()) {
            logger_->debug("Filesize: {} bytes", unixfsData.filesize());
        }
        logger_->debug("Blocksizes count: {}", unixfsData.blocksizes_size());
        for (int i = 0; i < unixfsData.blocksizes_size(); ++i) {
            logger_->debug("  Blocksize[{}]: {} bytes", i, unixfsData.blocksizes(i));
        }
        
        // Log raw UnixFS protobuf data for comparison
        std::ostringstream hexStream;
        for (unsigned char c : serialized) {
            hexStream << std::hex << std::setfill('0') << std::setw(2) << (unsigned)c << " ";
        }
        logger_->debug("Our raw UnixFS protobuf (hex): {}", hexStream.str());
        logger_->debug("==== End Our UnixFS Creation ====");

        return serialized;
    }

    CID Bitswap::createIPLDNode(const std::string& unixfsData, const std::map<std::string, CID>& links)
    {
        // Convert links to the format expected by the Kubo-compatible encoder (preserving order)
        std::vector<MerkledagLink> merkledagLinks;
        for (const auto& [name, cid] : links) {
            // Get the raw CID bytes
            auto cidResult = libp2p::multi::ContentIdentifierCodec::encode(cid);
            if (cidResult.has_value()) {
                MerkledagLink link;
                link.name = name;
                link.cid = std::move(cidResult.value());
                
                // Look up the total serialized size from the block store (tsize should be total IPLD node size)
                {
                    std::lock_guard<std::mutex> guard(mutexBlockStore_);
                    auto it = blockStore_.find(cid);
                    if (it != blockStore_.end()) {
                        link.tsize = it->second.size; // Total serialized IPLD size including UnixFS wrapper
                        logger_->debug("Setting tsize for link to CID {}: {} bytes", 
                                     libp2p::multi::ContentIdentifierCodec::toString(cid).value(), 
                                     link.tsize);
                    } else {
                        link.tsize = 0; // Fallback if not found in block store
                        logger_->warn("Could not find block size for CID {} in block store", 
                                    libp2p::multi::ContentIdentifierCodec::toString(cid).value());
                    }
                }
                
                merkledagLinks.push_back(std::move(link));
            }
        }
        
        // Use Kubo-compatible MerkleDAG encoder
        std::vector<uint8_t> encodedNode = MerkledagEncoder::encode(unixfsData, merkledagLinks);
        
        // Calculate CID for the encoded node
        auto cidBytes = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(encodedNode.data(), encodedNode.size());
        
        if (cidBytes.empty()) {
            logger_->error("Failed to create CID for IPLD node");
            throw std::runtime_error("Failed to create CID for IPLD node");
        }
        
        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span(reinterpret_cast<const uint8_t*>(cidBytes.data()), cidBytes.size()));
        
        if (!cid) {
            logger_->error("Failed to decode created CID: {}", cid.error().message());
            throw std::runtime_error("Failed to decode created CID: " + cid.error().message());
        }
        
        // Store the block
        std::string blockData(encodedNode.begin(), encodedNode.end());
        storeBlock(cid.value(), blockData);
        
        return cid.value();
    }

    CID Bitswap::createIPLDNode(const std::string& unixfsData, const std::vector<CID>& orderedChunkCIDs)
    {
        // Create MerkledagLinks from ordered chunk CIDs with empty names and correct tsize
        std::vector<MerkledagLink> merkledagLinks;
        for (const auto& cid : orderedChunkCIDs) {
            // Get the raw CID bytes
            auto cidResult = libp2p::multi::ContentIdentifierCodec::encode(cid);
            if (cidResult.has_value()) {
                MerkledagLink link;
                link.name = ""; // Kubo uses empty strings for chunk link names
                link.cid = std::move(cidResult.value());
                
                // Look up the total serialized size from the block store
                {
                    std::lock_guard<std::mutex> guard(mutexBlockStore_);
                    auto it = blockStore_.find(cid);
                    if (it != blockStore_.end()) {
                        link.tsize = it->second.size; // Total serialized IPLD size including UnixFS wrapper
                        logger_->debug("Setting tsize for link to CID {}: {} bytes", 
                                     libp2p::multi::ContentIdentifierCodec::toString(cid).value(), 
                                     link.tsize);
                    } else {
                        link.tsize = 0; // Fallback if not found in block store
                        logger_->warn("Could not find block size for CID {} in block store", 
                                    libp2p::multi::ContentIdentifierCodec::toString(cid).value());
                    }
                }
                
                merkledagLinks.push_back(std::move(link));
            }
        }
        
        // ===== DEBUG: LOG UNIXFS DATA AND LINKS BEFORE ENCODING =====
        logger_->debug("=== DETAILED ENCODING DEBUG ===");
        logger_->debug("UnixFS data size: {} bytes", unixfsData.size());
        
        // Log UnixFS data hex
        std::ostringstream unixfsHex;
        for (size_t i = 0; i < unixfsData.size(); ++i) {
            unixfsHex << std::hex << std::setfill('0') << std::setw(2) << (unsigned)unixfsData[i] << " ";
        }
        logger_->debug("UnixFS data (hex): {}", unixfsHex.str());
        
        logger_->debug("Number of links: {}", merkledagLinks.size());
        for (size_t i = 0; i < merkledagLinks.size(); ++i) {
            const auto& link = merkledagLinks[i];
            std::ostringstream cidHex;
            for (size_t j = 0; j < link.cid.size(); ++j) {
                cidHex << std::hex << std::setfill('0') << std::setw(2) << (unsigned)link.cid[j] << " ";
            }
            logger_->debug("Link[{}]: name='{}' tsize={} CID_bytes(hex)={}", 
                         i, link.name, link.tsize, cidHex.str());
        }
        
        // Use Kubo-compatible MerkleDAG encoder
        std::vector<uint8_t> encodedNode = MerkledagEncoder::encode(unixfsData, merkledagLinks);
        
        // Log encoded node hex
        std::ostringstream encodedHex;
        for (size_t i = 0; i < std::min(encodedNode.size(), static_cast<size_t>(100)); ++i) {
            encodedHex << std::hex << std::setfill('0') << std::setw(2) << (unsigned)encodedNode[i] << " ";
        }
        logger_->debug("Encoded IPLD node first 100 bytes (hex): {}", encodedHex.str());
        logger_->debug("=== END ENCODING DEBUG ===");
        
        // ===== ROUND-TRIP TEST FOR ENCODING/DECODING CONSISTENCY =====
        {
            // Test if our encoding can be properly decoded back using both string and vector methods
            std::string encodedAsString(encodedNode.begin(), encodedNode.end());
            
            MerkledagDecoder testDecoder1, testDecoder2;
            bool vectorDecodeSuccess = testDecoder1.decode(encodedNode);  // Direct vector
            bool stringDecodeSuccess = testDecoder2.decode(encodedAsString);  // String conversion
            
            logger_->debug("=== CHUNKED FILE ENCODING/DECODING ROUND-TRIP TEST ===");
            logger_->debug("Our encoded node size: {} bytes", encodedNode.size());
            logger_->debug("Vector decode success: {}", vectorDecodeSuccess);
            logger_->debug("String decode success: {}", stringDecodeSuccess);
            
            if (vectorDecodeSuccess && stringDecodeSuccess) {
                auto data1 = testDecoder1.getData();
                auto data2 = testDecoder2.getData();
                auto links1 = testDecoder1.getLinks();
                auto links2 = testDecoder2.getLinks();
                
                bool dataMatch = (data1.has_value() == data2.has_value()) && 
                               (!data1.has_value() || *data1 == *data2);
                bool linksMatch = (links1.size() == links2.size());
                
                logger_->debug("Data field matches: {}", dataMatch);
                logger_->debug("Links count matches: {} ({} vs {})", linksMatch, links1.size(), links2.size());
                logger_->debug("Round-trip consistency: {}", (dataMatch && linksMatch ? "GOOD" : "BAD"));
            } else {
                logger_->debug("Round-trip consistency: FAILED - decode errors");
            }
            logger_->debug("=== END CHUNKED FILE ROUND-TRIP TEST ===");
        }
        
        // Calculate CID for the encoded node
        auto cidBytes = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(encodedNode.data(), encodedNode.size());
        
        if (cidBytes.empty()) {
            logger_->error("Failed to create CID for IPLD node");
            throw std::runtime_error("Failed to create CID for IPLD node");
        }
        
        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span(reinterpret_cast<const uint8_t*>(cidBytes.data()), cidBytes.size()));
        
        if (!cid) {
            logger_->error("Failed to decode created CID: {}", cid.error().message());
            throw std::runtime_error("Failed to decode created CID: " + cid.error().message());
        }
        
        // Store the block
        std::string blockData(encodedNode.begin(), encodedNode.end());
        storeBlock(cid.value(), blockData);
        
        return cid.value();
    }

    CID Bitswap::createIPLDNodeAndStoreUnixFS(const std::string& unixfsData, const std::map<std::string, CID>& links)
    {
        // Convert links to the format expected by the Kubo-compatible encoder (preserving order)
        std::vector<MerkledagLink> merkledagLinks;
        for (const auto& [name, cid] : links) {
            // Get the raw CID bytes
            auto cidResult = libp2p::multi::ContentIdentifierCodec::encode(cid);
            if (cidResult.has_value()) {
                MerkledagLink link;
                link.name = name;
                link.cid = std::move(cidResult.value());
                link.tsize = 0; // Set to 0 as in original code
                merkledagLinks.push_back(std::move(link));
            }
        }
        
        // Use MerkledagEncoder for Kubo-compatible encoding
        std::vector<uint8_t> encodedNode = MerkledagEncoder::encode(unixfsData, merkledagLinks);
        
        // Calculate CID from the IPLD-encoded data (same way real IPFS nodes do)
        // This ensures the server and client calculate the same CID
        auto cidBytes = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(encodedNode.data(), encodedNode.size());
        
        if (cidBytes.empty()) {
            logger_->error("Failed to create CID for IPLD node");
            throw std::runtime_error("Failed to create CID for IPLD node");
        }
        
        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span(reinterpret_cast<const uint8_t*>(cidBytes.data()), cidBytes.size()));
        
        if (!cid) {
            logger_->error("Failed to decode created CID: {}", cid.error().message());
            throw std::runtime_error("Failed to decode created CID: " + cid.error().message());
        }
        
        // Store the IPLD-encoded data (what real IPFS nodes store and send)
        std::string ipldDataStr(encodedNode.begin(), encodedNode.end());
        storeBlock(cid.value(), ipldDataStr);
        
        return cid.value();
    }

    CID Bitswap::createIPLDNodeAndStoreRawData(const std::vector<uint8_t>& rawData)
    {
        // For individual data chunks, Kubo wraps them in UnixFS with type File (not Raw)
        // This creates the same wrapper that Kubo uses for chunks
        
        unixfs_pb::Data unixfsData;
        unixfsData.set_type(unixfs_pb::Data::File);
        unixfsData.set_data(rawData.data(), rawData.size());
        // Set filesize field to match Kubo's UnixFS structure exactly
        unixfsData.set_filesize(rawData.size());
        
        // Serialize UnixFS data using SerializeToArray for consistency with ParseFromArray
        //size_t serializedSize = unixfsData.ByteSizeLong();
        //std::vector<uint8_t> unixfsBytes(serializedSize);
        std::string unixfsString;
        if (!unixfsData.SerializeToString(&unixfsString)) {
            logger_->error("Failed to serialize UnixFS data for raw chunk");
            throw std::runtime_error("Failed to serialize UnixFS data for raw chunk");
        }
        
        // No links for individual chunks
        std::map<std::string, std::vector<uint8_t>> links;
        
        // Log UnixFS data before IPLD encoding
        logger_->debug("UnixFS data size: {} bytes", unixfsString.size());
        //logger_->debug("UnixFS hex: {}", bytesToHex(unixfsBytes));
        
        // Use Kubo-compatible MerkleDAG encoder (matches official go-merkledag protobuf)
        std::vector<uint8_t> encodedNode = MerkledagEncoder::encode(unixfsString, links);
        
        // Log IPLD encoded size and first/last bytes to understand the wrapper
        logger_->debug("IPLD encoded size: {} bytes", encodedNode.size());
        if (encodedNode.size() > 20) {
            std::vector<uint8_t> firstBytes(encodedNode.begin(), encodedNode.begin() + 10);
            std::vector<uint8_t> lastBytes(encodedNode.end() - 10, encodedNode.end());
            logger_->debug("IPLD first 10 bytes: {}", bytesToHex(firstBytes));
            logger_->debug("IPLD last 10 bytes: {}", bytesToHex(lastBytes));
        }
        
        // Detailed structure analysis
        analyzeIPLDStructure(encodedNode, "Our IPLD encoding");
        
        // Calculate CID for the encoded IPLD node
        auto cidResult = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(
            encodedNode.data(), encodedNode.size()
        );
        
        if (cidResult.empty()) {
            logger_->error("Failed to create CID for IPLD chunk");
            throw std::runtime_error("Failed to create CID for IPLD chunk");
        }
        
        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span(reinterpret_cast<const uint8_t*>(cidResult.data()), cidResult.size())
        );
        
        if (!cid) {
            logger_->error("Failed to decode created CID: {}", cid.error().message());
            throw std::runtime_error("Failed to decode created CID: " + cid.error().message());
        }
        
        // Store the IPLD-encoded data (matches Kubo's format exactly)
        std::string blockData(encodedNode.begin(), encodedNode.end());
        storeBlock(cid.value(), blockData);
        
        return cid.value();
    }

    void Bitswap::storeBlock(const CID& cid, const std::string& blockData, const std::string& originalPath)
    {
        std::lock_guard<std::mutex> guard(mutexBlockStore_);
        
        StoredBlock block{
            blockData,
            cid,
            originalPath.empty() ? std::nullopt : std::make_optional(originalPath),
            blockData.size(),
            std::chrono::steady_clock::now()
        };
        
        blockStore_.emplace(cid, std::move(block));
        
        logger_->debug("Stored block: {} ({} bytes)", cidToString(cid), blockData.size());
    }

    void Bitswap::handleWantlistRequest(const CID& wantedCid, std::shared_ptr<libp2p::connection::Stream> stream)
    {
        std::lock_guard<std::mutex> guard(mutexBlockStore_);
        
        auto blockIt = blockStore_.find(wantedCid);
        if (blockIt != blockStore_.end()) {
            logger_->debug("Responding to wantlist request for CID: {}", cidToString(wantedCid));
            
            // Send the block back to the requesting peer
            sendBlockResponse(wantedCid, blockIt->second.data, stream);
        } else {
            logger_->trace("Block not found for wantlist request: {}", cidToString(wantedCid));
        }
    }

    void Bitswap::sendBlockResponse(const CID& cid, const std::string& blockData, std::shared_ptr<libp2p::connection::Stream> stream)
    {
        // Create a bitswap message with the block
        bitswap_pb::Message pb_msg;
        BitswapMessage msg(pb_msg);
        
        logger_->debug("Preparing to send block response for CID: {} ({} bytes block data)", cidToString(cid), blockData.size());
        logger_->debug("Stream state before response: closed={}, closedForRead={}, closedForWrite={}", 
                      stream->isClosed(), stream->isClosedForRead(), stream->isClosedForWrite());
        
        // Add the block to the message - the blockData should be the raw content without CID prefix
        pb_msg.add_blocks(blockData);
        
        logger_->debug("Added block to protobuf message, total blocks in message: {}", pb_msg.blocks_size());
        
        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>(stream);
        rw->write<bitswap_pb::Message>(
            pb_msg,
            [ctx = shared_from_this(), cid, blockSize = blockData.size(), stream](auto&& writtenBytes) {
                if (writtenBytes) {
                    ctx->logger_->debug("Successfully sent block response for CID: {} ({} bytes written, {} block size)",
                                       cidToString(cid), writtenBytes.value(), blockSize);
                    ctx->logger_->debug("Stream state after response: closed={}, closedForRead={}, closedForWrite={}", 
                                       stream->isClosed(), stream->isClosedForRead(), stream->isClosedForWrite());
                } else {
                    ctx->logger_->error("Failed to send block response for CID: {}, error: {}",
                                       cidToString(cid), writtenBytes.error().message());
                }
            });
    }

    // Public API implementation

    void Bitswap::PublishFile(const std::string& filePath, PublishCallback onPublishCallback)
    {
        logger_->debug("Publishing file: {}", filePath);
        
        // Run encoding in a separate thread to avoid blocking
        std::thread([this, filePath, callback = std::move(onPublishCallback)]() {
            CID rootCID = encodeAndStoreFile(filePath);
            
            if (rootCID.content_address.toBuffer().empty()) {
                callback(BitswapError::ENCODING_FAILURE);
                return;
            }
            
            // Store published content info
            {
                std::lock_guard<std::mutex> guard(mutexBlockStore_);
                PublishedContent content{
                    filePath,
                    rootCID,
                    {},
                    UnixFSContent::SINGLE_FILE,
                    0,
                    std::chrono::steady_clock::now()
                };
                
                // Collect all blocks that belong to this content
                // For now, we'll just include the root block, but this should be enhanced
                // to include all related blocks for chunked files
                auto blockIt = blockStore_.find(rootCID);
                if (blockIt != blockStore_.end()) {
                    content.blocks.emplace(rootCID, blockIt->second);
                    content.totalSize = blockIt->second.size;
                }
                
                publishedContent_.emplace(rootCID, std::move(content));
            }
            
            logger_->info("Successfully published file: {} with CID: {}", filePath, cidToString(rootCID));
            
            callback(rootCID);
        }).detach();
    }

    void Bitswap::PublishDirectory(const std::string& directoryPath, PublishCallback onPublishCallback)
    {
        logger_->debug("Publishing directory: {}", directoryPath);
        
        // Run encoding in a separate thread to avoid blocking
        std::thread([this, directoryPath, callback = std::move(onPublishCallback)]() {
            CID rootCID = encodeAndStoreDirectory(directoryPath);
            
            if (rootCID.content_address.toBuffer().empty()) {
                callback(BitswapError::ENCODING_FAILURE);
                return;
            }
            
            // Store published content info
            {
                std::lock_guard<std::mutex> guard(mutexBlockStore_);
                PublishedContent content{
                    directoryPath,
                    rootCID,
                    {},
                    UnixFSContent::DIRECTORY,
                    0,
                    std::chrono::steady_clock::now()
                };
                
                // Collect all blocks that belong to this content
                // This should include all files and subdirectories
                size_t totalSize = 0;
                for (const auto& [cid, block] : blockStore_) {
                    if (block.filePath && block.filePath->find(directoryPath) == 0) {
                        content.blocks.emplace(cid, block);
                        totalSize += block.size;
                    }
                }
                content.totalSize = totalSize;
                
                publishedContent_.emplace(rootCID, std::move(content));
            }
            
            logger_->info("Successfully published directory: {} with CID: {}", directoryPath, cidToString(rootCID));
            
            callback(rootCID);
        }).detach();
    }

    void Bitswap::PublishData(const std::vector<uint8_t>& data, PublishCallback onPublishCallback)
    {
        logger_->debug("Publishing raw data: {} bytes", data.size());
        
        CID rootCID = encodeAndStoreData(data, unixfs_pb::Data::Raw);
        
        if (rootCID.content_address.toBuffer().empty()) {
            onPublishCallback(BitswapError::ENCODING_FAILURE);
            return;
        }
        
        // Store published content info
        {
            std::lock_guard<std::mutex> guard(mutexBlockStore_);
            PublishedContent content{
                "", // No file path for raw data
                rootCID,
                {},
                UnixFSContent::SINGLE_FILE,
                0,
                std::chrono::steady_clock::now()
            };
            
            auto blockIt = blockStore_.find(rootCID);
            if (blockIt != blockStore_.end()) {
                content.blocks.emplace(rootCID, blockIt->second);
                content.totalSize = blockIt->second.size;
            }
            
            publishedContent_.emplace(rootCID, std::move(content));
        }
        
        logger_->info("Successfully published raw data with CID: {}", cidToString(rootCID));
        
        onPublishCallback(rootCID);
    }

    bool Bitswap::HasBlock(const CID& cid) const
    {
        std::lock_guard<std::mutex> guard(mutexBlockStore_);
        return blockStore_.find(cid) != blockStore_.end();
    }

    libp2p::outcome::result<std::string> Bitswap::GetBlock(const CID& cid) const
    {
        std::lock_guard<std::mutex> guard(mutexBlockStore_);
        
        auto blockIt = blockStore_.find(cid);
        if (blockIt != blockStore_.end()) {
            return blockIt->second.data;
        }
        
        return BitswapError::BLOCK_NOT_FOUND;
    }

    bool Bitswap::UnpublishContent(const CID& rootCid)
    {
        std::lock_guard<std::mutex> guard(mutexBlockStore_);
        
        auto contentIt = publishedContent_.find(rootCid);
        if (contentIt == publishedContent_.end()) {
            return false;
        }
        
        // Remove all blocks associated with this content
        for (const auto& [cid, block] : contentIt->second.blocks) {
            blockStore_.erase(cid);
            logger_->debug("Removed block: {}", cidToString(cid));
        }
        
        publishedContent_.erase(contentIt);
        
        logger_->info("Unpublished content with root CID: {}", cidToString(rootCid));
        
        return true;
    }

    std::vector<PublishedContent> Bitswap::ListPublishedContent() const
    {
        std::lock_guard<std::mutex> guard(mutexBlockStore_);
        
        std::vector<PublishedContent> result;
        result.reserve(publishedContent_.size());
        
        for (const auto& [rootCid, content] : publishedContent_) {
            result.push_back(content);
        }
        
        return result;
    }

    // Provider management implementation

    void Bitswap::AddProvider(const CID& cid, const libp2p::peer::PeerInfo& peerInfo)
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto& providerList = providers_[cid];
        
        // Check if peer already exists in the list
        auto existingPeer = std::find_if(providerList.begin(), providerList.end(),
            [&peerInfo](const PeerProvider& provider) {
                return provider.peerInfo.id == peerInfo.id;
            });
        
        if (existingPeer != providerList.end()) {
            // Update existing peer info and mark as recently seen
            existingPeer->peerInfo = peerInfo;
            existingPeer->lastSeen = std::chrono::steady_clock::now();
            existingPeer->isReachable = true;
            logger_->debug("Updated existing provider {} for CID: {}", 
                          peerInfo.id.toBase58(), cidToString(cid));
        } else {
            // Add new provider
            providerList.emplace_back(peerInfo);
            logger_->debug("Added new provider {} for CID: {} (total: {})", 
                          peerInfo.id.toBase58(), cidToString(cid), providerList.size());
        }
    }

    void Bitswap::RemoveProvider(const CID& cid, const libp2p::peer::PeerId& peerId)
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto providerIt = providers_.find(cid);
        if (providerIt != providers_.end()) {
            auto& providerList = providerIt->second;
            
            auto peerIt = std::remove_if(providerList.begin(), providerList.end(),
                [&peerId](const PeerProvider& provider) {
                    return provider.peerInfo.id == peerId;
                });
            
            if (peerIt != providerList.end()) {
                providerList.erase(peerIt, providerList.end());
                logger_->debug("Removed provider {} for CID: {}", 
                              peerId.toBase58(), cidToString(cid));
                
                // Clean up empty provider lists
                if (providerList.empty()) {
                    providers_.erase(providerIt);
                }
            }
        }
    }

    std::vector<PeerProvider> Bitswap::GetProviders(const CID& cid) const
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto providerIt = providers_.find(cid);
        if (providerIt != providers_.end()) {
            return providerIt->second;
        }
        
        return {};
    }

    void Bitswap::ClearProviders(const CID& cid)
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        providers_.erase(cid);
        logger_->debug("Cleared all providers for CID: {}", cidToString(cid));
    }

    void Bitswap::SetMaxPeerAttempts(size_t maxPeers)
    {
        maxPeerAttempts_ = maxPeers;
        logger_->debug("Set max peer attempts to: {}", maxPeers);
    }

    void Bitswap::SetPeerFailureThreshold(int threshold)
    {
        peerFailureThreshold_ = threshold;
        logger_->debug("Set peer failure threshold to: {}", threshold);
    }

    void Bitswap::AddProviders(const CID& cid, const std::vector<libp2p::peer::PeerInfo>& peerInfos)
    {
        for (const auto& peerInfo : peerInfos) {
            AddProvider(cid, peerInfo);
        }
        logger_->debug("Added {} providers for CID: {}", peerInfos.size(), cidToString(cid));
    }

    size_t Bitswap::GetTotalProviderCount() const
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        size_t totalCount = 0;
        for (const auto& [cid, providers] : providers_) {
            totalCount += providers.size();
        }
        
        return totalCount;
    }

    std::map<std::string, std::vector<std::string>> Bitswap::GetProviderDebugInfo() const
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        std::map<std::string, std::vector<std::string>> debugInfo;
        
        for (const auto& [cid, providers] : providers_) {
            std::string cidStr = cidToString(cid);
            std::vector<std::string> providerInfos;
            
            for (const auto& provider : providers) {
                std::string info = provider.peerInfo.id.toBase58() + 
                    " (failures: " + std::to_string(provider.failureCount) + 
                    ", reachable: " + (provider.isReachable ? "yes" : "no") + ")";
                providerInfos.push_back(info);
            }
            
            debugInfo[cidStr] = std::move(providerInfos);
        }
        
        return debugInfo;
    }

    libp2p::peer::PeerInfo Bitswap::selectBestProvider(const CID& cid)
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto providerIt = providers_.find(cid);
        if (providerIt == providers_.end() || providerIt->second.empty()) {
            throw std::runtime_error("No providers available for CID");
        }
        
        auto& providerList = providerIt->second;
        
        // Filter out unreachable providers
        std::vector<std::reference_wrapper<PeerProvider>> reachableProviders;
        for (auto& provider : providerList) {
            if (provider.isReachable && provider.failureCount < peerFailureThreshold_) {
                reachableProviders.push_back(std::ref(provider));
            }
        }
        
        if (reachableProviders.empty()) {
            // Reset all providers and try again (maybe network conditions changed)
            logger_->warn("All providers marked as unreachable for CID: {}, resetting", cidToString(cid));
            for (auto& provider : providerList) {
                provider.isReachable = true;
                provider.failureCount = 0;
                reachableProviders.push_back(std::ref(provider));
            }
        }
        
        if (reachableProviders.empty()) {
            throw std::runtime_error("No reachable providers available for CID");
        }
        
        // Sort by failure count (ascending) and last seen (descending)
        std::sort(reachableProviders.begin(), reachableProviders.end(),
            [](const std::reference_wrapper<PeerProvider>& a, const std::reference_wrapper<PeerProvider>& b) {
                if (a.get().failureCount != b.get().failureCount) {
                    return a.get().failureCount < b.get().failureCount;
                }
                return a.get().lastSeen > b.get().lastSeen;
            });
        
        // Select a random provider from the best ones (top 25% or at least 1)
        size_t candidateCount = std::max(static_cast<size_t>(1), reachableProviders.size() / 4);
        size_t selectedIndex = std::rand() % candidateCount;
        
        auto& selectedProvider = reachableProviders[selectedIndex].get();
        selectedProvider.lastSeen = std::chrono::steady_clock::now();
        
        logger_->debug("Selected provider {} for CID: {} (failures: {}, total providers: {})", 
                      selectedProvider.peerInfo.id.toBase58(), cidToString(cid), 
                      selectedProvider.failureCount, providerList.size());
        
        return selectedProvider.peerInfo;
    }

    void Bitswap::markProviderFailure(const CID& cid, const libp2p::peer::PeerId& peerId)
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto providerIt = providers_.find(cid);
        if (providerIt != providers_.end()) {
            auto& providerList = providerIt->second;
            
            auto peerIt = std::find_if(providerList.begin(), providerList.end(),
                [&peerId](PeerProvider& provider) {
                    return provider.peerInfo.id == peerId;
                });
            
            if (peerIt != providerList.end()) {
                peerIt->failureCount++;
                if (peerIt->failureCount >= peerFailureThreshold_) {
                    peerIt->isReachable = false;
                    logger_->warn("Marked provider {} as unreachable for CID: {} (failures: {})", 
                                 peerId.toBase58(), cidToString(cid), peerIt->failureCount);
                } else {
                    logger_->debug("Incremented failure count for provider {} for CID: {} (failures: {})", 
                                  peerId.toBase58(), cidToString(cid), peerIt->failureCount);
                }
            }
        }
    }

    void Bitswap::markProviderSuccess(const CID& cid, const libp2p::peer::PeerId& peerId)
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto providerIt = providers_.find(cid);
        if (providerIt != providers_.end()) {
            auto& providerList = providerIt->second;
            
            auto peerIt = std::find_if(providerList.begin(), providerList.end(),
                [&peerId](PeerProvider& provider) {
                    return provider.peerInfo.id == peerId;
                });
            
            if (peerIt != providerList.end()) {
                peerIt->failureCount = std::max(0, peerIt->failureCount - 1); // Gradually reduce failure count
                peerIt->isReachable = true;
                peerIt->lastSeen = std::chrono::steady_clock::now();
                logger_->debug("Marked provider {} as successful for CID: {} (failures: {})", 
                              peerId.toBase58(), cidToString(cid), peerIt->failureCount);
            }
        }
    }

    void Bitswap::cleanupStaleProviders()
    {
        std::lock_guard<std::mutex> guard(mutexProviders_);
        
        auto now = std::chrono::steady_clock::now();
        auto staleThreshold = std::chrono::hours(1); // Remove providers not seen for 1 hour
        
        for (auto providerIt = providers_.begin(); providerIt != providers_.end();) {
            auto& providerList = providerIt->second;
            
            auto newEnd = std::remove_if(providerList.begin(), providerList.end(),
                [&now, &staleThreshold](const PeerProvider& provider) {
                    return (now - provider.lastSeen) > staleThreshold;
                });
            
            size_t removedCount = std::distance(newEnd, providerList.end());
            if (removedCount > 0) {
                logger_->debug("Removed {} stale providers for CID: {}", 
                              removedCount, cidToString(providerIt->first));
            }
            
            providerList.erase(newEnd, providerList.end());
            
            // Remove empty provider lists
            if (providerList.empty()) {
                providerIt = providers_.erase(providerIt);
            } else {
                ++providerIt;
            }
        }
    }

    void Bitswap::requestBlockWithProviders(const CID& cid, BlockCallback onBlockCallback, int attemptCount)
    {
        if (attemptCount >= static_cast<int>(maxPeerAttempts_)) {
            logger_->error("Exhausted all {} provider attempts for CID: {}", maxPeerAttempts_, cidToString(cid));
            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
            return;
        }

        // Try to find a provider for this CID
        try {
            auto selectedPeer = selectBestProvider(cid);
            logger_->debug("Attempting to request CID: {} from provider {} (attempt {})", 
                          cidToString(cid), selectedPeer.id.toBase58(), attemptCount + 1);

            // Make the request with failover handling
            RequestBlock(selectedPeer, cid, [this, cid, attemptCount, onBlockCallback = std::move(onBlockCallback)](libp2p::outcome::result<std::string> result) mutable {
                if (!result) {
                    logger_->warn("Request failed for CID: {} (attempt {}), trying next provider", 
                                 cidToString(cid), attemptCount + 1);
                    // Try the next provider
                    requestBlockWithProviders(cid, std::move(onBlockCallback), attemptCount + 1);
                } else {
                    // Success!
                    logger_->debug("Successfully received block for CID: {} on attempt {}", 
                                  cidToString(cid), attemptCount + 1);
                    onBlockCallback(std::move(result));
                }
            });
        } catch (const std::exception& e) {
            logger_->error("No providers available for CID: {} (attempt {})", cidToString(cid), attemptCount + 1);
            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
            return;
        }
    }

    void Bitswap::requestBlockWithProvidersFromRoot(const CID& rootCid, const CID& targetCid, BlockCallback onBlockCallback, int attemptCount)
    {
        if (attemptCount >= static_cast<int>(maxPeerAttempts_)) {
            logger_->error("Exhausted all {} provider attempts for target CID: {} using root CID: {}", 
                          maxPeerAttempts_, cidToString(targetCid), cidToString(rootCid));
            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
            return;
        }

        // Try to find a provider for the root CID (since that's what has providers registered)
        try {
            auto selectedPeer = selectBestProvider(rootCid);
            logger_->debug("Attempting to request target CID: {} from root CID provider {} (attempt {})", 
                          cidToString(targetCid), selectedPeer.id.toBase58(), attemptCount + 1);

            // Make the request for the target CID using the root CID's provider
            RequestBlock(selectedPeer, targetCid, [this, rootCid, targetCid, attemptCount, onBlockCallback = std::move(onBlockCallback)](libp2p::outcome::result<std::string> result) mutable {
                if (!result) {
                    logger_->warn("Request failed for target CID: {} using root CID provider (attempt {}), trying next provider", 
                                 cidToString(targetCid), attemptCount + 1);
                    // Try the next provider from the root CID
                    requestBlockWithProvidersFromRoot(rootCid, targetCid, std::move(onBlockCallback), attemptCount + 1);
                } else {
                    // Success!
                    logger_->debug("Successfully received block for target CID: {} using root CID provider on attempt {}", 
                                  cidToString(targetCid), attemptCount + 1);
                    onBlockCallback(std::move(result));
                }
            });
        } catch (const std::exception& e) {
            logger_->error("No providers available for root CID: {} when requesting target CID: {} (attempt {})", 
                          cidToString(rootCid), cidToString(targetCid), attemptCount + 1);
            onBlockCallback(BitswapError::OUTBOUND_STREAM_FAILURE);
            return;
        }
    }

}  // namespace sgns::ipfs_bitswap
