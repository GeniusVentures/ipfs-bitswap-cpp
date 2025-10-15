#include "bitswap.hpp"

#include "bitswap_message.hpp"
#include <proto/unixfs.pb.h>

#include <string>
#include <tuple>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include <ipfs_lite/ipld/impl/ipld_node_decoder_pb.hpp>
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

    void Bitswap::processUnixFSBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const std::string& blockData, const std::string& path)
    {
        if (ctx->timedOut) {
            return; // Don't process if already timed out
        }

        logger_->debug("Processing UnixFS block for CID: {}", libp2p::multi::ContentIdentifierCodec::toString(cid).value());

        // Mark this CID as completed
        ctx->pendingCIDs.erase(cid);
        ctx->completedCIDs.insert(cid);

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
        std::string filePath = path.empty() ? "" : path;
        
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
            return;
        }
        
        auto& fileProgress = fileIt->second;
        
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
        for (size_t i = 0; i < fileProgress.expectedChunks; ++i) {
            auto chunkIt = fileProgress.chunks.find(i);
            if (chunkIt != fileProgress.chunks.end()) {
                const auto& chunkData = chunkIt->second.data;
                completeFile.content.insert(completeFile.content.end(), chunkData.begin(), chunkData.end());
            } else {
                logger_->warn("Missing chunk {} for file {}", i, fileProgress.path);
            }
        }
        
        logger_->info("Assembled complete file: {} ({} bytes from {} chunks)", 
                     completeFile.path, completeFile.content.size(), fileProgress.chunks.size());
        
        // Add to collected files
        ctx->collectedFiles.push_back(std::move(completeFile));
        
        // Remove from files in progress
        ctx->filesInProgress.erase(fileCid);
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
            
            // Add to pending and track the path
            ctx->pendingCIDs.insert(linkCid.value());
            ctx->cidToPath[linkCid.value()] = childPath;
            
            logger_->debug("Requesting directory entry: {} -> {}", childPath, 
                          libp2p::multi::ContentIdentifierCodec::toString(linkCid.value()).value());
            
            // Request the linked content
            auto cidValue = linkCid.value(); // Extract the CID value
            RequestBlock(ctx->peerInfo.value(), cidValue, [this, ctx, cidValue, childPath](libp2p::outcome::result<std::string> entryResult) {
                if (!entryResult) {
                    if (!ctx->timedOut) {
                        logger_->error("Failed to fetch directory entry {}: {}", childPath, entryResult.error().message());
                        ctx->callback(entryResult.error());
                        std::lock_guard<std::mutex> guard(mutexContentRequests_);
                        contentRequests_.erase(ctx->rootCID);
                    }
                    return;
                }
                
                // Process the entry with the proper path
                processUnixFSBlock(ctx, cidValue, entryResult.value(), childPath);
            });
        }
        
        logger_->debug("Directory block processed: {} entries queued for processing", decoder.getLinksCount());
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
            
            processUnixFSBlock(ctx, nextCid, result.value());
            
            // Process next item in queue after a short delay
            auto timer = std::make_shared<boost::asio::deadline_timer>(*context_);
            timer->expires_from_now(boost::posix_time::milliseconds(50));
            timer->async_wait([this, ctx, timer](const boost::system::error_code& ec) {
                if (!ec && !ctx->timedOut) {
                    processRequestQueue(ctx);
                }
            });
        });
    }

}  // namespace sgns::ipfs_bitswap
