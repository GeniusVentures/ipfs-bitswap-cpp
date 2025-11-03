#include "bitswap.hpp"

#include "bitswap_message.hpp"
#include <proto/unixfs.pb.h>

#include <string>
#include <tuple>
#include <fstream>
#include <filesystem>
#include <thread>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include <ipfs_lite/ipld/impl/ipld_node_decoder_pb.hpp>
#include <ipfs_lite/ipld/impl/ipld_node_encoder_pb.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <common/buffer.hpp>

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
                        // We're a server that received a wantlist - don't set up additional reads
                        ctx->logger_->debug("Server side: processed wantlist, not setting up additional reads");
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
        auto decoder = ipfs_lite::ipld::IPLDNodeDecoderPB();
        auto byteSpan = gsl::span<const uint8_t>(reinterpret_cast<const uint8_t*>(blockData.data()), blockData.size());
        auto diddecode = decoder.decode(byteSpan);
        
        if (!diddecode) {
            if (!ctx->timedOut) {
                ctx->callback(BitswapError::IPLD_DECODE_FAILURE);
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                contentRequests_.erase(ctx->rootCID);
            }
            return;
        }

        // Process any links (additional CIDs to fetch)
        for (size_t i = 0; i < decoder.getLinksCount(); ++i) {
            auto linkCidData = decoder.getLinkCID(i);
            auto subcid = libp2p::multi::ContentIdentifierCodec::decode(
                gsl::span((uint8_t*)linkCidData.data(), linkCidData.size()));
            
            if (subcid && ctx->completedCIDs.find(subcid.value()) == ctx->completedCIDs.end()) {
                // We haven't processed this CID yet
                ctx->pendingCIDs.insert(subcid.value());
                
                logger_->debug("Found linked CID: {}, queuing...", libp2p::multi::ContentIdentifierCodec::toString(subcid.value()).value());
                
                // Add to queue instead of making immediate request
                ctx->requestQueue.push(subcid.value());
                processRequestQueue(ctx);
            }
        }

        // Parse UnixFS data from the IPLD content
        unixfs_pb::Data unixfsData;
        if (!unixfsData.ParseFromString(decoder.getContent())) {
            if (!ctx->timedOut) {
                ctx->callback(BitswapError::INVALID_UNIXFS_DATA);
                std::lock_guard<std::mutex> guard(mutexContentRequests_);
                contentRequests_.erase(ctx->rootCID);
            }
            return;
        }

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

    void Bitswap::handleFileBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const unixfs_pb::Data& unixfsData, const ipfs_lite::ipld::IPLDNodeDecoderPB& decoder, const std::string& path)
    {
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
        if (decoder.getLinksCount() == 0) {
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
            logger_->debug("Multi-chunk file detected: {} with {} chunks", filePath, decoder.getLinksCount());
            
            // Check if we already have this file in progress to prevent duplicates
            if (ctx->filesInProgress.find(cid) != ctx->filesInProgress.end()) {
                logger_->debug("File already in progress, skipping duplicate setup: {}", filePath);
                return;
            }
            
            ContentRequestContext::FileInProgress& fileProgress = ctx->filesInProgress[cid];
            fileProgress.path = filePath;
            fileProgress.expectedChunks = decoder.getLinksCount();
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
            for (size_t i = 0; i < decoder.getLinksCount(); ++i) {
                auto linkCidData = decoder.getLinkCID(i);
                auto linkCid = libp2p::multi::ContentIdentifierCodec::decode(
                    gsl::span((uint8_t*)linkCidData.data(), linkCidData.size()));
                
                if (linkCid && ctx->completedCIDs.find(linkCid.value()) == ctx->completedCIDs.end()) {
                    ctx->pendingCIDs.insert(linkCid.value());
                    ctx->cidToPath[linkCid.value()] = filePath; // Track which file this chunk belongs to
                    ctx->chunkToCidIndex[linkCid.value()] = ContentRequestContext::ChunkInfo(cid, i); // Track this is chunk i of file cid
                    
                    logger_->debug("Requesting file chunk {} for {}", i + 1, filePath);
                    
                    auto cidValue = linkCid.value(); // Extract the CID value
                    // Add to queue instead of making immediate request
                    ctx->requestQueue.push(cidValue);
                    processRequestQueue(ctx);
                } // Close if linkCid check
            } // Close for loop
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
        auto decoder = ipfs_lite::ipld::IPLDNodeDecoderPB();
        auto byteSpan = gsl::span((uint8_t*)chunkData.data(), chunkData.size());
        auto didDecode = decoder.decode(byteSpan);
        
        if (!didDecode) {
            logger_->error("Failed to decode IPLD chunk for file: {}", fileProgress.path);
            return;
        }
        
        // For file chunks, we typically want the raw content
        std::vector<char> chunkContent;
        
        // Try to parse as UnixFS data first
        unixfs_pb::Data unixfsData;
        if (unixfsData.ParseFromString(decoder.getContent()) && unixfsData.has_data()) {
            // This chunk contains UnixFS wrapped data
            chunkContent = std::vector<char>(unixfsData.data().begin(), unixfsData.data().end());
            logger_->debug("Decoded UnixFS chunk {} for {} ({} bytes)", chunkIndex, fileProgress.path, chunkContent.size());
        } else {
            // This might be raw data
            const auto& rawContent = decoder.getContent();
            chunkContent = std::vector<char>(rawContent.begin(), rawContent.end());
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

    void Bitswap::handleDirectoryBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const unixfs_pb::Data& unixfsData, const ipfs_lite::ipld::IPLDNodeDecoderPB& decoder, const std::string& basePath)
    {
        logger_->debug("Processing directory block with {} entries, basePath: '{}'", decoder.getLinksCount(), basePath);
        
        // Directory blocks contain links to their contents
        for (size_t i = 0; i < decoder.getLinksCount(); ++i) {
            auto linkName = decoder.getLinkName(i);
            auto linkCidData = decoder.getLinkCID(i);
            auto linkSize = decoder.getLinkSize(i);
            
            if (linkName.empty()) {
                logger_->warn("Directory entry {} has empty name, skipping", i);
                continue;
            }
            
            // Construct the full path for this entry
            std::string childPath = basePath.empty() ? linkName : basePath + "/" + linkName;
            
            // Parse the linked CID
            auto linkCid = libp2p::multi::ContentIdentifierCodec::decode(
                gsl::span((uint8_t*)linkCidData.data(), linkCidData.size()));
            
            if (!linkCid) {
                logger_->error("Failed to parse CID for directory entry: {}", childPath);
                continue;
            }
            
            // Check if we've already processed this CID
            if (ctx->completedCIDs.find(linkCid.value()) != ctx->completedCIDs.end()) {
                logger_->debug("Already processed CID for {}, skipping", childPath);
                continue;
            }
            
            // Always ensure the path mapping is set up, even for duplicates
            ctx->cidToPath[linkCid.value()] = childPath;
            
            // Check if we're already processing this CID
            if (ctx->pendingCIDs.find(linkCid.value()) != ctx->pendingCIDs.end()) {
                logger_->debug("Already pending CID for {}, skipping duplicate", childPath);
                continue;
            }
            
            // Add to pending
            ctx->pendingCIDs.insert(linkCid.value());
            
            logger_->debug("Requesting directory entry: {} -> {}", childPath, 
                          libp2p::multi::ContentIdentifierCodec::toString(linkCid.value()).value());
            
            // Add to request queue instead of making direct request
            auto cidValue = linkCid.value();
            ctx->requestQueue.push(cidValue);
        }
        
        logger_->debug("Directory block processed: {} entries queued for processing", decoder.getLinksCount());
        
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
        
        RequestBlock(ctx->peerInfo.value(), nextCid, [this, ctx, nextCid](libp2p::outcome::result<std::string> result) {
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

    CID Bitswap::encodeChunkedFile(const std::vector<uint8_t>& content, const std::string& filePath)
    {
        const size_t CHUNK_SIZE = 256 * 1024; // 256KB chunks
        std::vector<CID> chunkCIDs;
        std::vector<uint64_t> chunkSizes;
        
        // Create chunks
        for (size_t offset = 0; offset < content.size(); offset += CHUNK_SIZE) {
            size_t chunkSize = std::min(CHUNK_SIZE, content.size() - offset);
            std::vector<uint8_t> chunk(content.begin() + offset, content.begin() + offset + chunkSize);
            
            CID chunkCID = encodeAndStoreData(chunk, unixfs_pb::Data::Raw);
            if (chunkCID.content_address.toBuffer().empty()) {
                logger_->error("Failed to encode chunk for file: {}", filePath);
                throw std::runtime_error("Failed to encode chunk for file: " + filePath);
            }
            
            chunkCIDs.push_back(chunkCID);
            chunkSizes.push_back(chunkSize);
        }

        // Create root UnixFS node that links to all chunks
        unixfs_pb::Data unixfsData;
        unixfsData.set_type(unixfs_pb::Data::File);
        unixfsData.set_filesize(content.size());
        
        // Add blocksizes for each chunk
        for (uint64_t size : chunkSizes) {
            unixfsData.add_blocksizes(size);
        }

        // Serialize UnixFS data
        std::string serializedUnixFS;
        if (!unixfsData.SerializeToString(&serializedUnixFS)) {
            logger_->error("Failed to serialize UnixFS data for chunked file: {}", filePath);
            throw std::runtime_error("Failed to serialize UnixFS data for chunked file: " + filePath);
        }

        // Create IPLD links to chunks
        std::map<std::string, CID> links;
        for (size_t i = 0; i < chunkCIDs.size(); ++i) {
            links.emplace(std::to_string(i), chunkCIDs[i]);
        }

        // Create root IPLD node
        CID rootCID = createIPLDNode(std::vector<uint8_t>(serializedUnixFS.begin(), serializedUnixFS.end()), links);
        
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

        // Serialize UnixFS data
        std::string serializedUnixFS;
        if (!unixfsData.SerializeToString(&serializedUnixFS)) {
            logger_->error("Failed to serialize UnixFS data for directory: {}", directoryPath);
            throw std::runtime_error("Failed to serialize UnixFS data for directory: " + directoryPath);
        }

        // Create IPLD node
        CID dirCID = createIPLDNode(std::vector<uint8_t>(serializedUnixFS.begin(), serializedUnixFS.end()), links);
        
        logger_->debug("Created directory with {} entries, CID: {}", 
                      links.size(), cidToString(dirCID));
        
        return dirCID;
    }

    CID Bitswap::encodeAndStoreData(const std::vector<uint8_t>& data, unixfs_pb::Data::DataType type)
    {
        // Create UnixFS data
        std::vector<uint8_t> unixfsData = createUnixFSData(data, type, data.size());
        
        // Create IPLD node to calculate the CID, but store the UnixFS data for bitswap
        CID cid = createIPLDNodeAndStoreUnixFS(unixfsData);
        
        if (!cid.content_address.toBuffer().empty()) {
            logger_->debug("Encoded and stored {} bytes as CID: {}", data.size(), cidToString(cid));
        }
        
        return cid;
    }

    std::vector<uint8_t> Bitswap::createUnixFSData(const std::vector<uint8_t>& content, 
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

        std::string serialized;
        if (!unixfsData.SerializeToString(&serialized)) {
            logger_->error("Failed to serialize UnixFS data");
            return {};
        }

        return std::vector<uint8_t>(serialized.begin(), serialized.end());
    }

    CID Bitswap::createIPLDNode(const std::vector<uint8_t>& unixfsData, const std::map<std::string, CID>& links)
    {
        // Convert to common::Buffer for the encoder
        common::Buffer content(unixfsData);
        
        // Convert links to the format expected by the encoder
        std::map<std::string, ipfs_lite::ipld::IPLDLinkImpl> ipldLinks;
        for (const auto& [name, cid] : links) {
            // Convert to sgns::CID type and determine the size - for now use 0, should be improved
            sgns::CID sgnsCid(cid);
            ipldLinks[name] = ipfs_lite::ipld::IPLDLinkImpl(sgnsCid, name, 0);
        }
        
        // Encode IPLD node
        std::vector<uint8_t> encodedNode = ipfs_lite::ipld::IPLDNodeEncoderPB::encode(
            content, ipldLinks, std::set<std::string>{}
        );
        
        // Calculate CID for the encoded node
        auto cidResult = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(
            encodedNode.data(), encodedNode.size()
        );
        
        if (cidResult.empty()) {
            logger_->error("Failed to create CID for IPLD node");
            throw std::runtime_error("Failed to create CID for IPLD node");
        }
        
        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span(reinterpret_cast<const uint8_t*>(cidResult.data()), cidResult.size())
        );
        
        if (!cid) {
            logger_->error("Failed to decode created CID: {}", cid.error().message());
            throw std::runtime_error("Failed to decode created CID: " + cid.error().message());
        }
        
        // Store the block
        std::string blockData(encodedNode.begin(), encodedNode.end());
        storeBlock(cid.value(), blockData);
        
        return cid.value();
    }

    CID Bitswap::createIPLDNodeAndStoreUnixFS(const std::vector<uint8_t>& unixfsData, const std::map<std::string, CID>& links)
    {
        // Convert to common::Buffer for the encoder
        common::Buffer content(unixfsData);
        
        // Convert links to the format expected by the encoder
        std::map<std::string, ipfs_lite::ipld::IPLDLinkImpl> ipldLinks;
        for (const auto& [name, cid] : links) {
            // Convert to sgns::CID type and determine the size - for now use 0, should be improved
            sgns::CID sgnsCid(cid);
            ipldLinks[name] = ipfs_lite::ipld::IPLDLinkImpl(sgnsCid, name, 0);
        }
        
        // Encode IPLD node to calculate CID
        std::vector<uint8_t> encodedNode = ipfs_lite::ipld::IPLDNodeEncoderPB::encode(
            content, ipldLinks, std::set<std::string>{}
        );
        
        // Calculate CID for the encoded node
        auto cidResult = libp2p::multi::ContentIdentifierCodec::encodeCIDV0(
            encodedNode.data(), encodedNode.size()
        );
        
        if (cidResult.empty()) {
            logger_->error("Failed to create CID for IPLD node");
            throw std::runtime_error("Failed to create CID for IPLD node");
        }
        
        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span(reinterpret_cast<const uint8_t*>(cidResult.data()), cidResult.size())
        );
        
        if (!cid) {
            logger_->error("Failed to decode created CID: {}", cid.error().message());
            throw std::runtime_error("Failed to decode created CID: " + cid.error().message());
        }
        
        // Store the UnixFS data (not the IPLD-encoded data) for bitswap protocol
        std::string unixfsDataStr(unixfsData.begin(), unixfsData.end());
        storeBlock(cid.value(), unixfsDataStr);
        
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

}  // namespace sgns::ipfs_bitswap
