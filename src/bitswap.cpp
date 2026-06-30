#include "bitswap.hpp"

#include "bitswap_message.hpp"
#include "merkledag_encoder.hpp"
#include "merkledag_decoder.hpp"

#include <proto/unixfs.pb.h>
#include <proto/merkledag.pb.h>

#include <algorithm>
#include <memory>
#include <string>
#include <fstream>
#include <filesystem>
#include <thread>
#include <algorithm>
#include <functional>

#include <libp2p/basic/protobuf_message_read_writer.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/assert.hpp>

OUTCOME_CPP_DEFINE_CATEGORY( sgns::ipfs_bitswap, BitswapError, e )
{
    using sgns::ipfs_bitswap::BitswapError;
    switch ( e )
    {
        case BitswapError::OUTBOUND_STREAM_FAILURE:
            return "failed to create an outbound stream";
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
    constexpr size_t  CHUNK_SIZE        = 256 * 1024; // 256KB chunks
    const std::string bitswapProtocolId = "/ipfs/bitswap/1.0.0";

    std::string cidToString( const libp2p::multi::ContentIdentifier &cid )
    {
        auto result = libp2p::multi::ContentIdentifierCodec::toString( cid );
        return result.has_value() ? result.value() : "invalid";
    }
}

namespace sgns::ipfs_bitswap
{
    BitswapRequestContext::BitswapRequestContext( boost::asio::io_context &context ) :
        responseTimer_( context ), responseTimeout_( boost::posix_time::seconds( 5 ) )
    {
    }

    void BitswapRequestContext::AddCallback( BlockCallback callback )
    {
        responseTimer_.expires_from_now( responseTimeout_ );
        responseTimer_.async_wait( std::bind( &BitswapRequestContext::HandleResponseTimeout, this ) );
        callbacks_.emplace_back( std::move( callback ) );
    }

    void BitswapRequestContext::HandleResponse( libp2p::outcome::result<std::string> block )
    {
        responseTimer_.expires_at( boost::posix_time::pos_infin );
        for ( auto &callback : callbacks_ )
        {
            callback( block );
        }
        callbacks_.clear();
    }

    void BitswapRequestContext::HandleResponseTimeout()
    {
        HandleResponse( BitswapError::OUTBOUND_STREAM_FAILURE );
    }

    ContentRequestContext::ContentRequestContext( boost::asio::io_context &context, CID rootCid ) :
        rootCID( std::move( rootCid ) ), timeout( context ), contentTimeout_( boost::posix_time::seconds( 30 ) )
    {
    }

    Bitswap::Bitswap( libp2p::Host                            &host,
                      libp2p::event::Bus                      &eventBus,
                      std::shared_ptr<boost::asio::io_context> context ) :
        host_{ host }, bus_{ eventBus }, context_( std::move( context ) )
    {
    }

    void Bitswap::initialize()
    {
        host_.getRouter().setProtocolHandler(
            { getProtocolId() },
            [weak_self = std::weak_ptr<Bitswap>( shared_from_this() )]( auto stream_result )
            {
                if ( auto self = weak_self.lock() )
                {
                    self->handle( std::move( stream_result ) );
                }
            } );
        buildDiskIndex();
    }

    libp2p::peer::Protocol Bitswap::getProtocolId() const
    {
        return bitswapProtocolId;
    }

    void Bitswap::handle( libp2p::StreamAndProtocol rstream )
    {
        if ( !rstream.stream )
        {
            return;
        }

        auto &stream = rstream.stream;
        logStreamState( "accepted stream from peer", *stream );

        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>( stream );
        rw->read<bitswap_pb::Message>(
            [ctx = shared_from_this(), stream, rw]( libp2p::outcome::result<bitswap_pb::Message> rmsg )
            {
                if ( !rmsg )
                {
                    ctx->logger_->error( "bitswap message cannot be decoded" );
                    return;
                }

                BitswapMessage msg( rmsg.value() );

                bool hasWantlist = msg.GetWantlistSize() > 0;
                bool hasBlocks   = msg.GetBlocksSize() > 0;

                // Process wantlist requests (server side)
                for ( int i = 0; i < msg.GetWantlistSize(); ++i )
                {
                    auto blockId = msg.GetWantlistEntry( i ).block();
                    auto cid     = libp2p::multi::ContentIdentifierCodec::decode(
                        gsl::span( (uint8_t *)blockId.data(), blockId.size() ) );
                    if ( cid )
                    {
                        ctx->handleWantlistRequest( cid.value(), stream );
                    }
                }

                // Process blocks (client side)
                ctx->processReceivedBlocks( msg, stream );

                // Set up continuous reading for server mode
                if ( hasWantlist && !hasBlocks )
                {
                    std::function<void()> setupServerRead =
                        [ctx, stream, rw, setupServerRead = std::make_shared<std::function<void()>>()]() mutable
                    {
                        *setupServerRead = [ctx, stream, rw, setupServerRead]()
                        {
                            rw->read<bitswap_pb::Message>(
                                [ctx, stream, rw, setupServerRead](
                                    libp2p::outcome::result<bitswap_pb::Message> nextMsg )
                                {
                                    if ( !nextMsg )
                                    {
                                        return;
                                    }

                                    BitswapMessage nextBitswapMsg( nextMsg.value() );
                                    for ( int i = 0; i < nextBitswapMsg.GetWantlistSize(); ++i )
                                    {
                                        auto blockId = nextBitswapMsg.GetWantlistEntry( i ).block();
                                        auto cid     = libp2p::multi::ContentIdentifierCodec::decode(
                                            gsl::span( (uint8_t *)blockId.data(), blockId.size() ) );
                                        if ( cid )
                                        {
                                            ctx->handleWantlistRequest( cid.value(), stream );
                                        }
                                    }

                                    if ( nextBitswapMsg.GetWantlistSize() > 0 && nextBitswapMsg.GetBlocksSize() == 0 )
                                    {
                                        ( *setupServerRead )();
                                    }
                                } );
                        };
                        ( *setupServerRead )();
                    };
                    setupServerRead();
                }
                else if ( !hasWantlist && !hasBlocks )
                {
                    // Client waiting for blocks
                    rw->read<bitswap_pb::Message>(
                        [ctx, stream]( libp2p::outcome::result<bitswap_pb::Message> nextMsg )
                        {
                            if ( nextMsg )
                            {
                                BitswapMessage nextBitswapMsg( nextMsg.value() );
                                ctx->processReceivedBlocks( nextBitswapMsg, stream );
                            }
                        } );
                }
            } );
    }

    void Bitswap::start()
    {
        BOOST_ASSERT( !started_ );
        started_ = true;

        host_.setProtocolHandler( { bitswapProtocolId },
                                  [wp = weak_from_this()]( libp2p::StreamAndProtocol rstream )
                                  {
                                      if ( auto self = wp.lock() )
                                      {
                                          self->handle( std::move( rstream ) );
                                      }
                                  } );

        sub_ = bus_.getChannel<libp2p::event::network::OnNewConnectionChannel>().subscribe(
            [wp = weak_from_this()]( auto &&conn )
            {
                if ( auto self = wp.lock() )
                {
                    return self->onNewConnection( conn );
                }
            } );
    }

    void Bitswap::onNewConnection( const std::weak_ptr<libp2p::connection::CapableConnection> &conn )
    {
        if ( conn.expired() )
        {
            return;
        }
        auto remote_peer_res = conn.lock()->remotePeer();
        if ( remote_peer_res )
        {
            logger_->debug( "connected to peer {}", remote_peer_res.value().toBase58() );
        }
    }

    void Bitswap::processReceivedBlocks( const BitswapMessage                              &msg,
                                         const std::shared_ptr<libp2p::connection::Stream> &stream )
    {
        for ( int blockIdx = 0; blockIdx < msg.GetBlocksSize(); ++blockIdx )
        {
            const auto &block = msg.GetBlock( blockIdx );

            auto cidV0 = libp2p::multi::ContentIdentifierCodec::encodeCIDV0( block.data(), block.size() );
            auto cid   = libp2p::multi::ContentIdentifierCodec::decode(
                gsl::span( (uint8_t *)cidV0.data(), cidV0.size() ) );
            if ( !cid )
            {
                logger_->error( "CID cannot be decoded. {}", cid.error().message() );
                continue;
            }

            std::lock_guard<std::mutex> callbacksGuard( mutexRequestCallbacks_ );
            auto                        itContext = requestContexts_.find( cid.value() );
            if ( itContext != requestContexts_.end() )
            {
                if ( auto remotePeer = stream->remotePeerId() )
                {
                    markProviderSuccess( cid.value(), remotePeer.value() );
                }
                itContext->second->HandleResponse( block );
            }
            else
            {
                logger_->warn( "No request context found for received block CID: {}", cidToString( cid.value() ) );
            }
        }
    }

    void Bitswap::writeBitswapMessageToStream( std::shared_ptr<libp2p::connection::Stream> stream,
                                               const CID                                  &cid,
                                               BlockCallback                               onBlockCallback )
    {
        bitswap_pb::Message pb_msg;
        BitswapMessage      msg( pb_msg );
        msg.AddWantlistEntry( cid, true );

        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>( stream );
        rw->write<bitswap_pb::Message>(
            pb_msg,
            [ctx    = shared_from_this(),
             stream = std::move( stream ),
             cid( cid ),
             onBlockCallback = std::move( onBlockCallback )]( auto &&writtenBytes ) mutable
            { ctx->messageSent( writtenBytes, std::move( stream ), cid, std::move( onBlockCallback ) ); } );
    }

    void Bitswap::messageSent( libp2p::outcome::result<size_t>             writtenBytes,
                               std::shared_ptr<libp2p::connection::Stream> stream,
                               const CID                                  &cid,
                               BlockCallback                               onBlockCallback )
    {
        if ( !writtenBytes )
        {
            logger_->error( "cannot write bitswap message to stream to peer: {}", writtenBytes.error().message() );
            stream->reset();
            if ( auto remotePeer = stream->remotePeerId() )
            {
                markProviderFailure( cid, remotePeer.value() );
            }
            onBlockCallback( BitswapError::MESSAGE_SENDING_FAILURE );
            return;
        }

        {
            std::lock_guard<std::mutex> callbacksGuard( mutexRequestCallbacks_ );
            auto                        itCallbacks = requestContexts_.find( cid );
            if ( itCallbacks != requestContexts_.end() )
            {
                itCallbacks->second->AddCallback( std::move( onBlockCallback ) );
            }
            else
            {
                auto requestContext = std::make_shared<BitswapRequestContext>( *context_ );
                requestContext->AddCallback( std::move( onBlockCallback ) );
                requestContexts_.emplace( cid, std::move( requestContext ) );
            }
        }

        // Listen for server response
        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>( stream );
        rw->read<bitswap_pb::Message>(
            [ctx = shared_from_this(), stream]( libp2p::outcome::result<bitswap_pb::Message> responseMsg )
            {
                if ( responseMsg )
                {
                    BitswapMessage responseBitswapMsg( responseMsg.value() );
                    ctx->processReceivedBlocks( responseBitswapMsg, stream );
                }
            } );
    }

    void Bitswap::RequestContent( const libp2p::peer::PeerInfo &pi, const CID &cid, ContentCallback onContentCallback )
    {
        auto ctx      = setupContentRequest( cid, std::move( onContentCallback ), false );
        ctx->peerInfo = pi;

        RequestBlock( pi,
                      cid,
                      [this, ctx]( libp2p::outcome::result<std::string> blockResult )
                      {
                          if ( !blockResult )
                          {
                              failContentRequest( *ctx, static_cast<BitswapError>( blockResult.error().value() ) );
                              return;
                          }
                          processUnixFSBlock( ctx, ctx->rootCID, blockResult.value(), "" );
                      } );
    }

    void Bitswap::RequestContent( const CID &cid, ContentCallback onContentCallback )
    {
        auto ctx = setupContentRequest( cid, std::move( onContentCallback ), true );

        try
        {
            auto selectedPeer = selectBestProvider( cid );
            ctx->peerInfo     = selectedPeer;

            RequestBlock( selectedPeer,
                          cid,
                          [this, ctx]( libp2p::outcome::result<std::string> blockResult )
                          {
                              if ( !blockResult )
                              {
                                  failContentRequest( *ctx, static_cast<BitswapError>( blockResult.error().value() ) );
                                  return;
                              }
                              processUnixFSBlock( ctx, ctx->rootCID, blockResult.value(), "" );
                          } );
        }
        catch ( const std::exception & )
        {
            logger_->error( "No providers available for root CID: {}", cidToString( cid ) );
            failContentRequest( *ctx, BitswapError::OUTBOUND_STREAM_FAILURE );
        }
    }

    void Bitswap::RequestBlock( const libp2p::peer::PeerInfo &pi, const CID &cid, BlockCallback onBlockCallback )
    {
        RequestBlockWithRetry( pi, cid, std::move( onBlockCallback ), 0 );
    }

    void Bitswap::RequestBlockWithRetry( const libp2p::peer::PeerInfo &pi,
                                         const CID                    &cid,
                                         BlockCallback                 onBlockCallback,
                                         int                           retryCount )
    {
        const int maxRetries  = 2;
        const int baseDelayMs = 500;

        if ( host_.connectedness( pi ) == libp2p::Host::Connectedness::CAN_NOT_CONNECT )
        {
            logger_->debug( "Peer {} is not connectible", pi.id.toBase58() );
            onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
            return;
        }

        // Try reusing active stream
        {
            std::lock_guard<std::mutex> guard( mutexActiveStreams_ );
            auto                        streamIt = activeStreams_.find( pi.id );
            if ( streamIt != activeStreams_.end() )
            {
                if ( !streamIt->second->isClosed() )
                {
                    logger_->debug( "Reusing existing stream for peer {}", pi.id.toBase58() );
                    writeBitswapMessageToStream( streamIt->second, cid, std::move( onBlockCallback ) );
                    return;
                }
                activeStreams_.erase( streamIt );
            }
        }

        logger_->log(
            retryCount > 0 ? spdlog::level::warn : spdlog::level::debug,
            retryCount > 0 ? "Retrying stream creation for peer {} (attempt {}/{})" : "Creating new stream for peer {}",
            pi.id.toBase58(),
            retryCount + 1,
            maxRetries + 1 );

        host_.newStream(
            pi,
            { bitswapProtocolId },
            [wp = weak_from_this(),
             cid( cid ),
             pi( pi ),
             onBlockCallback = std::move( onBlockCallback ),
             retryCount,
             maxRetries,
             baseDelayMs]( libp2p::StreamAndProtocolOrError rstream ) mutable
            {
                auto ctx = wp.lock();
                if ( !ctx )
                {
                    return;
                }

                if ( !rstream )
                {
                    ctx->logger_->error( "Failed to create stream to peer {} (attempt {}): {}",
                                         pi.id.toBase58(),
                                         retryCount + 1,
                                         rstream.error().message() );
                    ctx->markProviderFailure( cid, pi.id );
                    {
                        std::lock_guard<std::mutex> guard( ctx->mutexActiveStreams_ );
                        ctx->activeStreams_.erase( pi.id );
                    }

                    if ( retryCount < maxRetries )
                    {
                        // Retry with exponential backoff
                        int  delay = baseDelayMs * ( 1 << retryCount );
                        auto timer = std::make_shared<boost::asio::deadline_timer>( *ctx->context_ );
                        timer->expires_from_now( boost::posix_time::milliseconds( delay ) );
                        timer->async_wait(
                            [ctx, pi, cid, onBlockCallback = std::move( onBlockCallback ), retryCount, timer](
                                const boost::system::error_code &ec ) mutable
                            {
                                if ( !ec )
                                {
                                    ctx->RequestBlockWithRetry( pi, cid, std::move( onBlockCallback ), retryCount + 1 );
                                }
                                else
                                {
                                    onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
                                }
                            } );
                    }
                    else
                    {
                        ctx->logger_->error( "All retry attempts failed for peer {}", pi.id.toBase58() );
                        onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
                    }
                }
                else
                {
                    auto stream = rstream.value().stream;
                    ctx->logStreamState( "outbound stream created", *stream );

                    // Cache the stream for reuse
                    {
                        std::lock_guard<std::mutex> guard( ctx->mutexActiveStreams_ );
                        ctx->activeStreams_[pi.id] = stream;
                    }
                    ctx->writeBitswapMessageToStream( std::move( stream ), cid, std::move( onBlockCallback ) );
                }
            },
            std::chrono::milliseconds( 5000 ) );
    }

    void Bitswap::logStreamState( const std::string_view &message, libp2p::connection::Stream &stream )
    {
        if ( logger_->should_log( spdlog::level::debug ) )
        {
            const auto remote_peer_res = stream.remotePeerId();
            const auto remote_addr_res = stream.remoteMultiaddr();
            const auto local_addr_res  = stream.localMultiaddr();

            logger_->debug( "{}: {}, {}, {}, isClosed: {}, canRead: {}, canWrite: {}",
                            message,
                            remote_peer_res ? remote_peer_res.value().toBase58() : "<no-remote-peer>",
                            remote_addr_res ? remote_addr_res.value().getStringAddress() : "<no-remote-addr>",
                            local_addr_res ? local_addr_res.value().getStringAddress() : "<no-local-addr>",
                            stream.isClosed(),
                            !stream.isClosedForRead(),
                            !stream.isClosedForWrite() );
        }
    }

    std::shared_ptr<ContentRequestContext> Bitswap::setupContentRequest( const CID      &cid,
                                                                         ContentCallback onContentCallback,
                                                                         bool            useProviders )
    {
        auto ctx          = std::make_shared<ContentRequestContext>( *context_, cid );
        ctx->callback     = std::move( onContentCallback );
        ctx->useProviders = useProviders;
        ctx->pendingCIDs.insert( cid );

        {
            std::lock_guard<std::mutex> guard( mutexContentRequests_ );
            contentRequests_[cid] = ctx;
        }

        // Set up timeout
        ctx->timeout.expires_from_now( ctx->contentTimeout_ );
        ctx->timeout.async_wait(
            [this, cid]( const boost::system::error_code &ec )
            {
                if ( ec )
                {
                    return;
                }
                std::lock_guard<std::mutex> guard( mutexContentRequests_ );
                auto                        it = contentRequests_.find( cid );
                if ( it != contentRequests_.end() && !it->second->timedOut )
                {
                    it->second->timedOut = true;
                    it->second->callback( BitswapError::CONTENT_REQUEST_TIMEOUT );
                    contentRequests_.erase( it );
                }
            } );

        return ctx;
    }

    void Bitswap::failContentRequest( ContentRequestContext &ctx, BitswapError error )
    {
        if ( ctx.timedOut )
        {
            return;
        }
        ctx.callback( error );
        std::lock_guard<std::mutex> guard( mutexContentRequests_ );
        contentRequests_.erase( ctx.rootCID );
    }

    void Bitswap::processUnixFSBlock( std::shared_ptr<ContentRequestContext> ctx,
                                      const CID                             &cid,
                                      const std::string                     &blockData,
                                      const std::string                     &path )
    {
        if ( ctx->timedOut || ctx->completedCIDs.count( cid ) != 0 )
        {
            return;
        }

        ctx->pendingCIDs.erase( cid );
        ctx->completedCIDs.insert( cid );

        // Check if this CID is a chunk of a file in progress
        auto chunkIt = ctx->chunkToCidIndex.find( cid );
        if ( chunkIt != ctx->chunkToCidIndex.end() && chunkIt->second.parentCid.has_value() )
        {
            handleFileChunk( ctx, cid, blockData, chunkIt->second.chunkIndex, chunkIt->second.parentCid.value() );
            checkContentRequestComplete( ctx );
            return;
        }

        MerkledagDecoder decoder;
        if ( !decoder.decode( blockData ) )
        {
            failContentRequest( *ctx, BitswapError::IPLD_DECODE_FAILURE );
            return;
        }

        // Queue any new links
        for ( const auto &link : decoder.getLinks() )
        {
            if ( ctx->completedCIDs.count( link.cid ) == 0 )
            {
                ctx->pendingCIDs.insert( link.cid );
                ctx->requestQueue.push( link.cid );
                processRequestQueue( ctx );
            }
        }

        // Parse UnixFS data
        unixfs_pb::Data unixfsData;
        auto            dataOpt = decoder.getData();
        if ( !dataOpt || !unixfsData.ParseFromArray( dataOpt->data(), static_cast<int>( dataOpt->size() ) ) )
        {
            failContentRequest( *ctx, BitswapError::INVALID_UNIXFS_DATA );
            return;
        }

        switch ( unixfsData.type() )
        {
            case unixfs_pb::Data::Directory:
                handleDirectoryBlock( ctx, decoder, path );
                break;
            default:
                handleFileBlock( ctx, cid, unixfsData, decoder, path );
                break;
        }

        checkContentRequestComplete( std::move( ctx ) );
    }

    void Bitswap::handleFileBlock( std::shared_ptr<ContentRequestContext> ctx,
                                   const CID                             &cid,
                                   const unixfs_pb::Data                 &unixfsData,
                                   const MerkledagDecoder                &decoder,
                                   const std::string                     &path )
    {
        auto links = decoder.getLinks();

        std::string filePath = path;
        if ( filePath.empty() )
        {
            auto pathIt = ctx->cidToPath.find( cid );
            if ( pathIt != ctx->cidToPath.end() )
            {
                filePath = pathIt->second;
            }
        }

        if ( links.empty() )
        {
            // Complete file in single block
            if ( unixfsData.has_data() )
            {
                UnixFSFile file;
                file.path    = filePath;
                file.content = std::vector<char>( unixfsData.data().begin(), unixfsData.data().end() );
                file.size    = unixfsData.has_filesize() ? unixfsData.filesize() : unixfsData.data().size();
                if ( unixfsData.has_mode() )
                {
                    file.mode = unixfsData.mode();
                }
                if ( unixfsData.has_mtime() )
                {
                    file.mtime = unixfsData.mtime();
                }
                ctx->collectedFiles.push_back( std::move( file ) );
            }
            return;
        }

        // Multi-chunk file, in progress
        if ( ctx->filesInProgress.count( cid ) )
        {
            return;
        }

        auto &fileProgress          = ctx->filesInProgress[cid];
        fileProgress.path           = filePath;
        fileProgress.expectedChunks = links.size();
        fileProgress.totalSize      = unixfsData.has_filesize() ? unixfsData.filesize() : 0;
        if ( unixfsData.has_mode() )
        {
            fileProgress.mode = unixfsData.mode();
        }
        if ( unixfsData.has_mtime() )
        {
            fileProgress.mtime = unixfsData.mtime();
        }

        if ( unixfsData.has_data() )
        {
            fileProgress.chunks[0] = ContentRequestContext::FileChunk{
                std::vector<char>( unixfsData.data().begin(), unixfsData.data().end() ),
                0,
                std::make_optional( cid ) };
        }

        for ( size_t i = 0; i < links.size(); ++i )
        {
            if ( ctx->completedCIDs.count( links[i].cid ) == 0 )
            {
                ctx->pendingCIDs.insert( links[i].cid );
                ctx->cidToPath[links[i].cid]       = filePath;
                ctx->chunkToCidIndex[links[i].cid] = ContentRequestContext::ChunkInfo{ cid, i };
                ctx->requestQueue.push( links[i].cid );
                processRequestQueue( ctx );
            }
        }
    }

    void Bitswap::handleFileChunk( std::shared_ptr<ContentRequestContext> ctx,
                                   const CID                             &chunkCid,
                                   const std::string                     &chunkData,
                                   size_t                                 chunkIndex,
                                   const CID                             &parentCid )
    {
        ctx->completedCIDs.insert( chunkCid );
        ctx->pendingCIDs.erase( chunkCid );

        auto fileIt = ctx->filesInProgress.find( parentCid );
        if ( fileIt == ctx->filesInProgress.end() )
        {
            logger_->error( "Received chunk for unknown file CID: {}", cidToString( parentCid ) );
            auto pathIt = ctx->cidToPath.find( parentCid );
            if ( pathIt != ctx->cidToPath.end() )
            {
                logger_->debug( "Attempting to recreate file progress entry for: {}", pathIt->second );
                auto &fileProgress          = ctx->filesInProgress[parentCid];
                fileProgress.path           = pathIt->second;
                fileProgress.expectedChunks = 1;
            }
            else
            {
                logger_->debug( "Assembled empty directory" );
                return;
            }
        }

        auto &fileProgress = ctx->filesInProgress[parentCid];

        MerkledagDecoder decoder;
        if ( !decoder.decode( chunkData ) )
        {
            logger_->error( "Failed to decode IPLD chunk for file: {}", fileProgress.path );
            return;
        }

        std::vector<char> chunkContent;
        unixfs_pb::Data   unixfsData;
        auto              dataOpt = decoder.getData();

        if ( dataOpt && unixfsData.ParseFromArray( dataOpt->data(), static_cast<int>( dataOpt->size() ) ) &&
             unixfsData.has_data() )
        {
            chunkContent = std::vector<char>( unixfsData.data().begin(), unixfsData.data().end() );
        }
        else if ( dataOpt )
        {
            chunkContent = std::vector<char>( dataOpt->begin(), dataOpt->end() );
        }

        fileProgress.chunks[chunkIndex] = ContentRequestContext::FileChunk{ std::move( chunkContent ),
                                                                            chunkIndex,
                                                                            std::make_optional( chunkCid ) };
        fileProgress.expectedChunks     = std::max( chunkIndex + 1, fileProgress.expectedChunks );

        if ( fileProgress.chunks.size() >= fileProgress.expectedChunks )
        {
            assembleCompleteFile( std::move( ctx ), parentCid, fileProgress );
        }
    }

    void Bitswap::assembleCompleteFile( std::shared_ptr<ContentRequestContext>       ctx,
                                        const CID                                   &fileCid,
                                        const ContentRequestContext::FileInProgress &fileProgress )
    {
        UnixFSFile completeFile;
        completeFile.path  = fileProgress.path;
        completeFile.mode  = fileProgress.mode;
        completeFile.mtime = fileProgress.mtime;

        size_t totalContentSize = 0;
        for ( const auto &[index, chunk] : fileProgress.chunks )
        {
            totalContentSize += chunk.data.size();
        }

        completeFile.content.reserve( totalContentSize );
        completeFile.size = fileProgress.totalSize > 0 ? fileProgress.totalSize : totalContentSize;

        size_t missingChunks = 0;
        for ( size_t i = 0; i < fileProgress.expectedChunks; ++i )
        {
            auto chunkIt = fileProgress.chunks.find( i );
            if ( chunkIt != fileProgress.chunks.end() )
            {
                completeFile.content.insert( completeFile.content.end(),
                                             chunkIt->second.data.begin(),
                                             chunkIt->second.data.end() );
            }
            else
            {
                logger_->warn( "Missing chunk {} for file {}", i, fileProgress.path );
                missingChunks++;
            }
        }

        if ( missingChunks > 0 )
        {
            logger_->warn( "File {} assembled with {} missing chunks out of {}",
                           fileProgress.path,
                           missingChunks,
                           fileProgress.expectedChunks );
        }

        logger_->info( "Assembled complete file: {} ({} bytes from {}/{} chunks)",
                       completeFile.path,
                       completeFile.content.size(),
                       fileProgress.chunks.size(),
                       fileProgress.expectedChunks );

        ctx->collectedFiles.push_back( std::move( completeFile ) );
        ctx->filesInProgress.erase( fileCid );

        // Clear chunk mappings for this file
        for ( auto it = ctx->chunkToCidIndex.begin(); it != ctx->chunkToCidIndex.end(); )
        {
            if ( it->second.parentCid == fileCid )
            {
                it = ctx->chunkToCidIndex.erase( it );
            }
            else
            {
                ++it;
            }
        }
    }

    void Bitswap::handleDirectoryBlock( std::shared_ptr<ContentRequestContext> ctx,
                                        const MerkledagDecoder                &decoder,
                                        const std::string                     &basePath )
    {
        for ( const auto &link : decoder.getLinks() )
        {
            if ( link.name.empty() )
            {
                logger_->warn( "Directory entry has empty name, skipping" );
                continue;
            }

            std::string childPath = basePath.empty() ? link.name : basePath + "/" + link.name;

            if ( ctx->completedCIDs.count( link.cid ) != 0 )
            {
                continue;
            }

            ctx->cidToPath[link.cid] = childPath;

            if ( ctx->pendingCIDs.count( link.cid ) != 0 )
            {
                continue;
            }

            ctx->pendingCIDs.insert( link.cid );
            ctx->requestQueue.push( link.cid );
        }
        processRequestQueue( std::move( ctx ) );
    }

    void Bitswap::checkContentRequestComplete( std::shared_ptr<ContentRequestContext> ctx )
    {
        if ( ctx->timedOut || !ctx->pendingCIDs.empty() || !ctx->filesInProgress.empty() )
        {
            return;
        }

        UnixFSContent content = assembleContent( ctx );
        ctx->timeout.cancel();
        ctx->callback( std::move( content ) );

        std::lock_guard<std::mutex> guard( mutexContentRequests_ );
        contentRequests_.erase( ctx->rootCID );
    }

    UnixFSContent Bitswap::assembleContent( std::shared_ptr<ContentRequestContext> ctx )
    {
        UnixFSContent content;

        if ( ctx->collectedFiles.empty() )
        {
            content.type = UnixFSContent::DIRECTORY;
        }
        else if ( ctx->collectedFiles.size() == 1 && ctx->collectedFiles[0].path.empty() )
        {
            content.type = UnixFSContent::SINGLE_FILE;
        }
        else
        {
            bool hasDirStructure = std::any_of(
                ctx->collectedFiles.begin(),
                ctx->collectedFiles.end(),
                []( const UnixFSFile &f ) { return !f.path.empty() && f.path.find( '/' ) != std::string::npos; } );
            content.type = hasDirStructure ? UnixFSContent::MULTI_FILE_ARCHIVE : UnixFSContent::DIRECTORY;
        }

        content.files                   = std::move( ctx->collectedFiles );
        content.metadata["root_cid"]    = cidToString( ctx->rootCID );
        content.metadata["total_files"] = std::to_string( content.files.size() );

        size_t totalSize = 0;
        for ( const auto &file : content.files )
        {
            totalSize += file.content.size();
        }
        content.metadata["total_size"] = std::to_string( totalSize );

        return content;
    }

    void Bitswap::processRequestQueue( std::shared_ptr<ContentRequestContext> ctx )
    {
        if ( ctx->processingQueue || ctx->requestQueue.empty() || ctx->timedOut )
        {
            return;
        }

        ctx->processingQueue = true;
        auto nextCid         = ctx->requestQueue.front();
        ctx->requestQueue.pop();

        logger_->debug( "Processing queued request for CID: {}", cidToString( nextCid ) );

        if ( ctx->peerInfo.has_value() )
        {
            RequestBlock( ctx->peerInfo.value(),
                          nextCid,
                          [this, ctx, nextCid]( libp2p::outcome::result<std::string> result )
                          {
                              if ( !result && ctx->useProviders )
                              {
                                  // Fallback to provider system
                                  ctx->processingQueue = false;
                                  requestBlockWithProvidersFromRoot(
                                      ctx->rootCID,
                                      nextCid,
                                      [this, ctx, nextCid]( libp2p::outcome::result<std::string> fallbackResult )
                                      { handleQueuedBlockResult( ctx, nextCid, std::move( fallbackResult ) ); } );
                                  return;
                              }
                              handleQueuedBlockResult( ctx, nextCid, std::move( result ) );
                          } );
        }
        else if ( ctx->useProviders )
        {
            requestBlockWithProviders( nextCid,
                                       [this, ctx, nextCid]( libp2p::outcome::result<std::string> result )
                                       { handleQueuedBlockResult( ctx, nextCid, std::move( result ) ); } );
        }
        else
        {
            logger_->error( "No peer info and providers disabled for CID: {}", cidToString( nextCid ) );
            ctx->processingQueue = false;
            failContentRequest( *ctx, BitswapError::OUTBOUND_STREAM_FAILURE );
        }
    }

    void Bitswap::handleQueuedBlockResult( std::shared_ptr<ContentRequestContext> ctx,
                                           const CID                             &nextCid,
                                           libp2p::outcome::result<std::string>   result )
    {
        ctx->processingQueue = false;

        if ( !result )
        {
            failContentRequest( *ctx, static_cast<BitswapError>( result.error().value() ) );
            return;
        }

        std::string path;
        auto        pathIt = ctx->cidToPath.find( nextCid );
        if ( pathIt != ctx->cidToPath.end() )
        {
            path = pathIt->second;
        }

        processUnixFSBlock( ctx, nextCid, result.value(), path );

        // Process next item in queue after a short delay
        auto timer = std::make_shared<boost::asio::deadline_timer>( *context_ );
        timer->expires_from_now( boost::posix_time::milliseconds( 200 ) );
        timer->async_wait(
            [this, ctx, timer]( const boost::system::error_code &ec )
            {
                if ( !ec && !ctx->timedOut )
                {
                    processRequestQueue( ctx );
                }
            } );
    }

    CID Bitswap::encodeAndStoreFile( const std::string &filePath )
    {
        namespace fs = std::filesystem;

        if ( !fs::exists( filePath ) || !fs::is_regular_file( filePath ) )
        {
            logger_->error( "File not found or not a regular file: {}", filePath );
            throw std::runtime_error( "File not found: " + filePath );
        }

        std::ifstream file( filePath, std::ios::binary );
        if ( !file )
        {
            logger_->error( "Failed to open file: {}", filePath );
            throw std::runtime_error( "Failed to open file: " + filePath );
        }

        file.seekg( 0, std::ios::end );
        auto fileSize = file.tellg();
        file.seekg( 0, std::ios::beg );

        std::vector<uint8_t> content( fileSize );
        file.read( reinterpret_cast<char *>( content.data() ), fileSize );

        return ( fileSize <= CHUNK_SIZE ) ? encodeAndStoreData( content, unixfs_pb::Data::File )
                                          : encodeChunkedFile( content, filePath );
    }

    CID Bitswap::encodeChunkedFile( const std::vector<uint8_t> &content, const std::string &filePath )
    {
        std::vector<CID>      chunkCIDs;
        std::vector<uint64_t> chunkSizes;

        for ( size_t offset = 0; offset < content.size(); offset += CHUNK_SIZE )
        {
            size_t               rawChunkSize = std::min( CHUNK_SIZE, content.size() - offset );
            std::vector<uint8_t> chunk( content.begin() + offset, content.begin() + offset + rawChunkSize );

            // Store chunk as UnixFS File-type IPLD node (matches Kubo)
            unixfs_pb::Data chunkUnixfs;
            chunkUnixfs.set_type( unixfs_pb::Data::File );
            chunkUnixfs.set_data( chunk.data(), chunk.size() );
            chunkUnixfs.set_filesize( chunk.size() );

            std::string serializedChunk;
            if ( !chunkUnixfs.SerializeToString( &serializedChunk ) )
            {
                throw std::runtime_error( "Failed to serialize UnixFS data for chunk" );
            }

            CID chunkCID = encodeAndStoreMerkledagNode( serializedChunk, {} );
            if ( chunkCID.content_address.toBuffer().empty() )
            {
                throw std::runtime_error( "Failed to encode chunk for file: " + filePath );
            }

            chunkSizes.push_back( chunk.size() );
            chunkCIDs.push_back( chunkCID );
        }

        // Root node linking all chunks
        unixfs_pb::Data unixfsData;
        unixfsData.set_type( unixfs_pb::Data::File );
        unixfsData.set_filesize( content.size() );
        for ( uint64_t size : chunkSizes )
        {
            unixfsData.add_blocksizes( size );
        }

        std::string serializedUnixFS;
        if ( !unixfsData.SerializeToString( &serializedUnixFS ) )
        {
            throw std::runtime_error( "Failed to serialize UnixFS data for chunked file: " + filePath );
        }

        // Build links with empty names (Kubo convention for chunk links)
        std::vector<MerkledagLink> merkledagLinks;
        merkledagLinks.reserve( chunkCIDs.size() );
        for ( const auto &cid : chunkCIDs )
        {
            auto cidResult = libp2p::multi::ContentIdentifierCodec::encode( cid );
            if ( !cidResult.has_value() )
            {
                continue;
            }

            MerkledagLink link;
            link.name = "";
            link.cid  = std::move( cidResult.value() );
            {
                std::lock_guard<std::mutex> guard( mutexBlockStore_ );
                auto                        it = blockStore_.find( cid );
                link.tsize                     = ( it != blockStore_.end() ) ? it->second.size : 0;
            }
            merkledagLinks.push_back( std::move( link ) );
        }

        CID rootCID = encodeAndStoreMerkledagNode( serializedUnixFS, merkledagLinks, content.size() );

        // Update contentSize to include total IPLD overhead
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            size_t                      totalSizeWithOverhead = 0;
            for ( const auto &chunkCID : chunkCIDs )
            {
                auto it = blockStore_.find( chunkCID );
                if ( it != blockStore_.end() )
                {
                    totalSizeWithOverhead += it->second.size;
                }
            }
            auto rootIt = blockStore_.find( rootCID );
            if ( rootIt != blockStore_.end() )
            {
                totalSizeWithOverhead      += rootIt->second.size;
                rootIt->second.contentSize  = totalSizeWithOverhead;
            }
        }

        return rootCID;
    }

    CID Bitswap::encodeAndStoreDirectory( const std::string &directoryPath )
    {
        namespace fs = std::filesystem;

        if ( !fs::exists( directoryPath ) || !fs::is_directory( directoryPath ) )
        {
            throw std::runtime_error( "Directory not found: " + directoryPath );
        }

        unixfs_pb::Data unixfsData;
        unixfsData.set_type( unixfs_pb::Data::Directory );

        std::map<std::string, CID> links;

        for ( const auto &entry : fs::directory_iterator( directoryPath ) )
        {
            std::string entryName = entry.path().filename().string();
            try
            {
                CID entryCID = entry.is_regular_file() ? encodeAndStoreFile( entry.path().string() )
                               : entry.is_directory()  ? encodeAndStoreDirectory( entry.path().string() )
                                                       : throw std::runtime_error( "Unsupported file type" );

                if ( !entryCID.content_address.toBuffer().empty() )
                {
                    links.emplace( entryName, entryCID );
                }
            }
            catch ( const std::exception &e )
            {
                logger_->warn( "Failed to process directory entry {}: {}", entry.path().string(), e.what() );
            }
        }

        std::string serializedUnixFS;
        if ( !unixfsData.SerializeToString( &serializedUnixFS ) )
        {
            throw std::runtime_error( "Failed to serialize UnixFS data for directory: " + directoryPath );
        }

        // Build directory links with contentSize for tsize
        std::vector<MerkledagLink> merkledagLinks;
        size_t                     totalDirectoryContentSize = 0;
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            for ( const auto &[name, cid] : links )
            {
                auto cidResult = libp2p::multi::ContentIdentifierCodec::encode( cid );
                if ( !cidResult.has_value() )
                {
                    continue;
                }

                MerkledagLink link;
                link.name = name;
                link.cid  = std::move( cidResult.value() );

                auto it = blockStore_.find( cid );
                if ( it != blockStore_.end() )
                {
                    link.tsize                 = it->second.contentSize;
                    totalDirectoryContentSize += it->second.contentSize;
                }
                else
                {
                    link.tsize = 0;
                    logger_->warn( "DIRECTORY LINK MISSING: {} -> CID {} NOT FOUND in block store (using tsize=0)",
                                   name,
                                   cidToString( cid ) );
                }
                merkledagLinks.push_back( std::move( link ) );
            }
        }

        CID dirCID = encodeAndStoreMerkledagNode( serializedUnixFS, merkledagLinks );

        // Update contentSize to include directory's own IPLD size
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            auto                        it = blockStore_.find( dirCID );
            if ( it != blockStore_.end() )
            {
                it->second.contentSize = totalDirectoryContentSize + it->second.size;
            }
        }

        return dirCID;
    }

    CID Bitswap::encodeAndStoreData( const std::vector<uint8_t> &data, unixfs_pb::Data::DataType type )
    {
        unixfs_pb::Data unixfsData;
        unixfsData.set_type( type );
        if ( !data.empty() )
        {
            unixfsData.set_data( data.data(), data.size() );
        }
        if ( data.size() > 0 && ( type == unixfs_pb::Data::File || type == unixfs_pb::Data::Raw ) )
        {
            unixfsData.set_filesize( data.size() );
        }

        std::string serialized;
        if ( !unixfsData.SerializeToString( &serialized ) )
        {
            logger_->error( "Failed to serialize UnixFS data" );
        }

        return encodeAndStoreMerkledagNode( serialized, {} );
    }

    CID Bitswap::encodeAndStoreMerkledagNode( const std::string &unixfsData, const std::vector<MerkledagLink> &links )
    {
        std::vector<uint8_t> encodedNode = MerkledagEncoder::encode( unixfsData, links );

        auto cidBytes = libp2p::multi::ContentIdentifierCodec::encodeCIDV0( encodedNode.data(), encodedNode.size() );
        if ( cidBytes.empty() )
        {
            throw std::runtime_error( "Failed to create CID for IPLD node" );
        }

        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span( reinterpret_cast<const uint8_t *>( cidBytes.data() ), cidBytes.size() ) );
        if ( !cid )
        {
            throw std::runtime_error( "Failed to decode created CID: " + cid.error().message() );
        }

        std::string blockData( encodedNode.begin(), encodedNode.end() );
        storeBlock( cid.value(), blockData );

        return cid.value();
    }

    CID Bitswap::encodeAndStoreMerkledagNode( const std::string                &unixfsData,
                                              const std::vector<MerkledagLink> &links,
                                              size_t                            contentSize )
    {
        std::vector<uint8_t> encodedNode = MerkledagEncoder::encode( unixfsData, links );

        auto cidBytes = libp2p::multi::ContentIdentifierCodec::encodeCIDV0( encodedNode.data(), encodedNode.size() );
        if ( cidBytes.empty() )
        {
            throw std::runtime_error( "Failed to create CID for IPLD node" );
        }

        auto cid = libp2p::multi::ContentIdentifierCodec::decode(
            gsl::span( reinterpret_cast<const uint8_t *>( cidBytes.data() ), cidBytes.size() ) );
        if ( !cid )
        {
            throw std::runtime_error( "Failed to decode created CID: " + cid.error().message() );
        }

        std::string blockData( encodedNode.begin(), encodedNode.end() );
        storeBlock( cid.value(), blockData, "", contentSize );

        return cid.value();
    }

    void Bitswap::storeBlock( const CID         &cid,
                              const std::string &blockData,
                              const std::string &originalPath,
                              size_t             contentSize )
    {
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            blockStore_.emplace( cid,
                                 StoredBlock{ blockData,
                                              cid,
                                              originalPath.empty() ? std::nullopt : std::make_optional( originalPath ),
                                              blockData.size(),
                                              contentSize > 0 ? contentSize : blockData.size(),
                                              std::chrono::steady_clock::now() } );
        }
        persistBlock( cid, blockData );
    }

    void Bitswap::handleWantlistRequest( const CID &wantedCid, std::shared_ptr<libp2p::connection::Stream> stream )
    {
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            auto                        blockIt = blockStore_.find( wantedCid );
            if ( blockIt != blockStore_.end() )
            {
                sendBlockResponse( wantedCid, blockIt->second.data, stream );
                return;
            }
        }
        // Lazy-load from disk if available
        if ( tryLoadFromDisk( wantedCid ) )
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            auto                        blockIt = blockStore_.find( wantedCid );
            if ( blockIt != blockStore_.end() )
            {
                sendBlockResponse( wantedCid, blockIt->second.data, stream );
                return;
            }
        }
    }

    void Bitswap::sendBlockResponse( const CID                                  &cid,
                                     const std::string                          &blockData,
                                     std::shared_ptr<libp2p::connection::Stream> stream )
    {
        bitswap_pb::Message pb_msg;
        pb_msg.add_blocks( blockData );

        auto rw = std::make_shared<libp2p::basic::ProtobufMessageReadWriter>( stream );
        rw->write<bitswap_pb::Message>( pb_msg,
                                        [ctx = shared_from_this(), cid, stream]( auto &&writtenBytes )
                                        {
                                            if ( !writtenBytes )
                                            {
                                                ctx->logger_->error(
                                                    "Failed to send block response for CID: {}, error: {}",
                                                    cidToString( cid ),
                                                    writtenBytes.error().message() );
                                            }
                                        } );
    }

    void Bitswap::PublishFile( const std::string &filePath, PublishCallback onPublishCallback )
    {
        logger_->debug( "Publishing file: {}", filePath );

        std::thread(
            [this, filePath, callback = std::move( onPublishCallback )]()
            {
                CID rootCID = encodeAndStoreFile( filePath );
                if ( rootCID.content_address.toBuffer().empty() )
                {
                    callback( BitswapError::ENCODING_FAILURE );
                    return;
                }

                {
                    std::lock_guard<std::mutex> guard( mutexBlockStore_ );
                    PublishedContent            content{ filePath,
                                                         rootCID,
                                                         {},
                                                         UnixFSContent::SINGLE_FILE,
                                                         0,
                                                         std::chrono::steady_clock::now() };
                    auto                        blockIt = blockStore_.find( rootCID );
                    if ( blockIt != blockStore_.end() )
                    {
                        content.blocks.emplace( rootCID, blockIt->second );
                        content.totalSize = blockIt->second.size;
                    }
                    publishedContent_.emplace( rootCID, std::move( content ) );
                }

                logger_->info( "Successfully published file: {} with CID: {}", filePath, cidToString( rootCID ) );
                callback( rootCID );
            } )
            .detach();
    }

    void Bitswap::PublishDirectory( const std::string &directoryPath, PublishCallback onPublishCallback )
    {
        logger_->debug( "Publishing directory: {}", directoryPath );

        std::thread(
            [this, directoryPath, callback = std::move( onPublishCallback )]()
            {
                CID rootCID = encodeAndStoreDirectory( directoryPath );
                if ( rootCID.content_address.toBuffer().empty() )
                {
                    callback( BitswapError::ENCODING_FAILURE );
                    return;
                }

                {
                    std::lock_guard<std::mutex> guard( mutexBlockStore_ );
                    PublishedContent            content{ directoryPath,
                                                         rootCID,
                                                         {},
                                                         UnixFSContent::DIRECTORY,
                                                         0,
                                                         std::chrono::steady_clock::now() };
                    size_t                      totalSize = 0;
                    for ( const auto &[cid, block] : blockStore_ )
                    {
                        if ( block.filePath && block.filePath->find( directoryPath ) == 0 )
                        {
                            content.blocks.emplace( cid, block );
                            totalSize += block.size;
                        }
                    }
                    content.totalSize = totalSize;
                    publishedContent_.emplace( rootCID, std::move( content ) );
                }

                logger_->info( "Successfully published directory: {} with CID: {}",
                               directoryPath,
                               cidToString( rootCID ) );
                callback( rootCID );
            } )
            .detach();
    }

    void Bitswap::PublishData( const std::vector<uint8_t> &data, PublishCallback onPublishCallback )
    {
        logger_->debug( "Publishing raw data: {} bytes", data.size() );

        CID rootCID = encodeAndStoreData( data, unixfs_pb::Data::Raw );
        if ( rootCID.content_address.toBuffer().empty() )
        {
            onPublishCallback( BitswapError::ENCODING_FAILURE );
            return;
        }

        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            PublishedContent            content{ "",
                                                 rootCID,
                                                 {},
                                                 UnixFSContent::SINGLE_FILE,
                                                 0,
                                                 std::chrono::steady_clock::now() };
            auto                        blockIt = blockStore_.find( rootCID );
            if ( blockIt != blockStore_.end() )
            {
                content.blocks.emplace( rootCID, blockIt->second );
                content.totalSize = blockIt->second.size;
            }
            publishedContent_.emplace( rootCID, std::move( content ) );
        }

        logger_->info( "Successfully published raw data with CID: {}", cidToString( rootCID ) );
        onPublishCallback( rootCID );
    }

    bool Bitswap::HasBlock( const CID &cid ) const
    {
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            if ( blockStore_.count( cid ) > 0 )
            {
                return true;
            }
        }
        // Check disk index as fallback
        auto cidStr = cidToString( cid );
        std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
        return diskIndex_.count( cidStr ) > 0;
    }

    libp2p::outcome::result<std::string> Bitswap::GetBlock( const CID &cid ) const
    {
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            auto                        blockIt = blockStore_.find( cid );
            if ( blockIt != blockStore_.end() )
            {
                return blockIt->second.data;
            }
        }
        // Lazy-load from disk if available
        if ( const_cast<Bitswap *>( this )->tryLoadFromDisk( cid ) )
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            auto                        blockIt = blockStore_.find( cid );
            if ( blockIt != blockStore_.end() )
            {
                return blockIt->second.data;
            }
        }
        return BitswapError::BLOCK_NOT_FOUND;
    }

    bool Bitswap::UnpublishContent( const CID &rootCid )
    {
        std::lock_guard<std::mutex> guard( mutexBlockStore_ );
        auto                        contentIt = publishedContent_.find( rootCid );
        if ( contentIt == publishedContent_.end() )
        {
            return false;
        }

        for ( const auto &[cid, block] : contentIt->second.blocks )
        {
            blockStore_.erase( cid );
            logger_->debug( "Removed block: {}", cidToString( cid ) );
        }
        publishedContent_.erase( contentIt );
        logger_->info( "Unpublished content with root CID: {}", cidToString( rootCid ) );
        return true;
    }

    std::vector<PublishedContent> Bitswap::ListPublishedContent() const
    {
        std::lock_guard<std::mutex>   guard( mutexBlockStore_ );
        std::vector<PublishedContent> result;
        result.reserve( publishedContent_.size() );
        for ( const auto &[rootCid, content] : publishedContent_ )
        {
            result.push_back( content );
        }
        return result;
    }

    PeerProvider *Bitswap::findProvider( const CID &cid, const libp2p::peer::PeerId &peerId )
    {
        auto providerIt = providers_.find( cid );
        if ( providerIt == providers_.end() )
        {
            return nullptr;
        }
        auto peerIt = std::find_if( providerIt->second.begin(),
                                    providerIt->second.end(),
                                    [&peerId]( const PeerProvider &p ) { return p.peerInfo.id == peerId; } );
        return ( peerIt != providerIt->second.end() ) ? &( *peerIt ) : nullptr;
    }

    void Bitswap::AddProvider( const CID &cid, libp2p::peer::PeerInfo peerInfo )
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );

        auto &providerList = providers_[cid];
        auto *existing     = findProvider( cid, peerInfo.id );

        if ( existing )
        {
            existing->peerInfo    = peerInfo;
            existing->lastSeen    = std::chrono::steady_clock::now();
            existing->isReachable = true;
            logger_->debug( "Updated existing provider {} for CID: {}", peerInfo.id.toBase58(), cidToString( cid ) );
        }
        else
        {
            providerList.emplace_back( peerInfo );
            logger_->debug( "Added new provider {} for CID: {} (total: {})",
                            peerInfo.id.toBase58(),
                            cidToString( cid ),
                            providerList.size() );
        }
    }

    void Bitswap::RemoveProvider( const CID &cid, const libp2p::peer::PeerId &peerId )
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );
        auto                        providerIt = providers_.find( cid );
        if ( providerIt == providers_.end() )
        {
            return;
        }

        auto &providerList = providerIt->second;
        auto  newEnd       = std::remove_if( providerList.begin(),
                                             providerList.end(),
                                             [&peerId]( const PeerProvider &p ) { return p.peerInfo.id == peerId; } );

        if ( newEnd != providerList.end() )
        {
            providerList.erase( newEnd, providerList.end() );
            logger_->debug( "Removed provider {} for CID: {}", peerId.toBase58(), cidToString( cid ) );
            if ( providerList.empty() )
            {
                providers_.erase( providerIt );
            }
        }
    }

    std::vector<PeerProvider> Bitswap::GetProviders( const CID &cid ) const
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );
        auto                        providerIt = providers_.find( cid );
        if ( providerIt != providers_.end() )
        {
            return providerIt->second;
        }

        return {};
    }

    void Bitswap::ClearProviders( const CID &cid )
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );
        providers_.erase( cid );
        logger_->debug( "Cleared all providers for CID: {}", cidToString( cid ) );
    }

    void Bitswap::SetMaxPeerAttempts( size_t maxPeers )
    {
        maxPeerAttempts_ = maxPeers;
    }

    void Bitswap::SetPeerFailureThreshold( int threshold )
    {
        peerFailureThreshold_ = threshold;
    }

    void Bitswap::AddProviders( const CID &cid, const std::vector<libp2p::peer::PeerInfo> &peerInfos )
    {
        for ( const auto &peerInfo : peerInfos )
        {
            AddProvider( cid, peerInfo );
        }
        logger_->debug( "Added {} providers for CID: {}", peerInfos.size(), cidToString( cid ) );
    }

    size_t Bitswap::GetTotalProviderCount() const
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );
        size_t                      total = 0;
        for ( const auto &[cid, providers] : providers_ )
        {
            total += providers.size();
        }
        return total;
    }

    std::map<std::string, std::vector<std::string>> Bitswap::GetProviderDebugInfo() const
    {
        std::lock_guard<std::mutex>                     guard( mutexProviders_ );
        std::map<std::string, std::vector<std::string>> debugInfo;

        for ( const auto &[cid, providers] : providers_ )
        {
            std::vector<std::string> infos;
            for ( const auto &p : providers )
            {
                std::string info = p.peerInfo.id.toBase58() + " (failures: " + std::to_string( p.failureCount ) +
                                   ", reachable: " + ( p.isReachable ? "yes" : "no" ) + ")";
                infos.push_back( info );
            }
            debugInfo[cidToString( cid )] = std::move( infos );
        }
        return debugInfo;
    }

    libp2p::peer::PeerInfo Bitswap::selectBestProvider( const CID &cid )
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );

        auto providerIt = providers_.find( cid );
        if ( providerIt == providers_.end() || providerIt->second.empty() )
        {
            throw std::runtime_error( "No providers available for CID" );
        }

        auto &providerList = providerIt->second;

        std::vector<std::reference_wrapper<PeerProvider>> reachableProviders;
        for ( auto &p : providerList )
        {
            if ( p.isReachable && p.failureCount < peerFailureThreshold_ )
            {
                reachableProviders.push_back( std::ref( p ) );
            }
        }

        if ( reachableProviders.empty() )
        {
            logger_->warn( "All providers marked as unreachable for CID: {}, resetting", cidToString( cid ) );
            for ( auto &p : providerList )
            {
                p.isReachable  = true;
                p.failureCount = 0;
                reachableProviders.push_back( std::ref( p ) );
            }
        }

        if ( reachableProviders.empty() )
        {
            throw std::runtime_error( "No reachable providers available for CID" );
        }

        std::sort( reachableProviders.begin(),
                   reachableProviders.end(),
                   []( const auto &a, const auto &b )
                   {
                       if ( a.get().failureCount != b.get().failureCount )
                       {
                           return a.get().failureCount < b.get().failureCount;
                       }
                       return a.get().lastSeen > b.get().lastSeen;
                   } );

        size_t candidateCount = std::max( static_cast<size_t>( 1 ), reachableProviders.size() / 4 );
        size_t selectedIndex  = std::rand() % candidateCount;
        auto  &selected       = reachableProviders[selectedIndex].get();
        selected.lastSeen     = std::chrono::steady_clock::now();

        logger_->debug( "Selected provider {} for CID: {} (failures: {}, total: {})",
                        selected.peerInfo.id.toBase58(),
                        cidToString( cid ),
                        selected.failureCount,
                        providerList.size() );

        return selected.peerInfo;
    }

    void Bitswap::markProviderFailure( const CID &cid, const libp2p::peer::PeerId &peerId )
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );
        auto                       *p = findProvider( cid, peerId );
        if ( !p )
        {
            return;
        }

        p->failureCount++;
        if ( p->failureCount >= peerFailureThreshold_ )
        {
            p->isReachable = false;
            logger_->warn( "Marked provider {} as unreachable for CID: {} (failures: {})",
                           peerId.toBase58(),
                           cidToString( cid ),
                           p->failureCount );
        }
        else
        {
            logger_->debug( "Incremented failure count for provider {} for CID: {} (failures: {})",
                            peerId.toBase58(),
                            cidToString( cid ),
                            p->failureCount );
        }
    }

    void Bitswap::markProviderSuccess( const CID &cid, const libp2p::peer::PeerId &peerId )
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );
        auto                       *p = findProvider( cid, peerId );
        if ( !p )
        {
            return;
        }

        p->failureCount = std::max( 0, p->failureCount - 1 );
        p->isReachable  = true;
        p->lastSeen     = std::chrono::steady_clock::now();
        logger_->debug( "Marked provider {} as successful for CID: {} (failures: {})",
                        peerId.toBase58(),
                        cidToString( cid ),
                        p->failureCount );
    }

    void Bitswap::cleanupStaleProviders()
    {
        std::lock_guard<std::mutex> guard( mutexProviders_ );

        auto now            = std::chrono::steady_clock::now();
        auto staleThreshold = std::chrono::hours( 1 );

        for ( auto providerIt = providers_.begin(); providerIt != providers_.end(); )
        {
            auto &pl     = providerIt->second;
            auto  newEnd = std::remove_if( pl.begin(),
                                           pl.end(),
                                           [&]( const PeerProvider &p )
                                           { return ( now - p.lastSeen ) > staleThreshold; } );

            size_t removedCount = std::distance( newEnd, pl.end() );
            if ( removedCount > 0 )
            {
                logger_->debug( "Removed {} stale providers for CID: {}",
                                removedCount,
                                cidToString( providerIt->first ) );
            }

            pl.erase( newEnd, pl.end() );
            if ( pl.empty() )
            {
                providerIt = providers_.erase( providerIt );
            }
            else
            {
                ++providerIt;
            }
        }
    }

    void Bitswap::requestBlockWithProviders( const CID &cid, BlockCallback onBlockCallback, int attemptCount )
    {
        if ( attemptCount >= static_cast<int>( maxPeerAttempts_ ) )
        {
            logger_->error( "Exhausted all {} provider attempts for CID: {}", maxPeerAttempts_, cidToString( cid ) );
            onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
            return;
        }

        try
        {
            auto selectedPeer = selectBestProvider( cid );
            logger_->debug( "Attempting to request CID: {} from provider {} (attempt {})",
                            cidToString( cid ),
                            selectedPeer.id.toBase58(),
                            attemptCount + 1 );

            RequestBlock( selectedPeer,
                          cid,
                          [this, cid, attemptCount, onBlockCallback = std::move( onBlockCallback )](
                              libp2p::outcome::result<std::string> result ) mutable
                          {
                              if ( !result )
                              {
                                  logger_->warn( "Request failed for CID: {} (attempt {}), trying next provider",
                                                 cidToString( cid ),
                                                 attemptCount + 1 );
                                  requestBlockWithProviders( cid, std::move( onBlockCallback ), attemptCount + 1 );
                              }
                              else
                              {
                                  onBlockCallback( std::move( result ) );
                              }
                          } );
        }
        catch ( const std::exception & )
        {
            logger_->error( "No providers available for CID: {} (attempt {})", cidToString( cid ), attemptCount + 1 );
            onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
        }
    }

    void Bitswap::requestBlockWithProvidersFromRoot( const CID    &rootCid,
                                                     const CID    &targetCid,
                                                     BlockCallback onBlockCallback,
                                                     int           attemptCount )
    {
        if ( attemptCount >= static_cast<int>( maxPeerAttempts_ ) )
        {
            logger_->error( "Exhausted all {} provider attempts for target CID: {} using root CID: {}",
                            maxPeerAttempts_,
                            cidToString( targetCid ),
                            cidToString( rootCid ) );
            onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
            return;
        }

        try
        {
            auto selectedPeer = selectBestProvider( rootCid );
            logger_->debug( "Attempting to request target CID: {} from root CID provider {} (attempt {})",
                            cidToString( targetCid ),
                            selectedPeer.id.toBase58(),
                            attemptCount + 1 );

            RequestBlock( selectedPeer,
                          targetCid,
                          [this, rootCid, targetCid, attemptCount, onBlockCallback](
                              libp2p::outcome::result<std::string> result ) mutable
                          {
                              if ( !result )
                              {
                                  logger_->warn(
                                      "Request failed for target CID: {} using root CID provider (attempt {})",
                                      cidToString( targetCid ),
                                      attemptCount + 1 );
                                  requestBlockWithProvidersFromRoot( rootCid,
                                                                     targetCid,
                                                                     std::move( onBlockCallback ),
                                                                     attemptCount + 1 );
                              }
                              else
                              {
                                  onBlockCallback( std::move( result ) );
                              }
                          } );
        }
        catch ( const std::exception & )
        {
            logger_->error( "No providers available for root CID: {} when requesting target CID: {} (attempt {})",
                            cidToString( rootCid ),
                            cidToString( targetCid ),
                            attemptCount + 1 );
            onBlockCallback( BitswapError::OUTBOUND_STREAM_FAILURE );
        }
    }

    // ===================================================================
    // Disk Persistence Implementation
    // ===================================================================

    void Bitswap::setCacheDir( const std::string &dir )
    {
        cacheDir_ = dir;
        if ( !dir.empty() )
        {
            logger_->info( "Bitswap disk cache directory set to: {}", dir );
        }
    }

    std::string Bitswap::getCacheDir() const
    {
        return cacheDir_;
    }

    void Bitswap::buildDiskIndex()
    {
        if ( cacheDir_.empty() )
        {
            return;
        }

        namespace fs = std::filesystem;
        if ( !fs::exists( cacheDir_ ) || !fs::is_directory( cacheDir_ ) )
        {
            logger_->debug( "Bitswap cache directory does not exist, skipping disk index build: {}", cacheDir_ );
            return;
        }

        {
            std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
            diskIndex_.clear();
        }

        size_t count = 0;
        for ( const auto &entry : fs::directory_iterator( cacheDir_ ) )
        {
            if ( entry.is_regular_file() )
            {
                std::string cidStr = entry.path().filename().string();
                {
                    std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
                    diskIndex_.insert( cidStr );
                }
                ++count;
            }
        }

        logger_->info( "Built disk index from {}: {} CIDs available", cacheDir_, count );
    }

    std::string Bitswap::cidToFilePath( const std::string &cidStr ) const
    {
        return cacheDir_ + "/" + cidStr;
    }

    void Bitswap::persistBlock( const CID &cid, const std::string &blockData )
    {
        if ( cacheDir_.empty() )
        {
            return;
        }

        auto cidStr = cidToString( cid );
        if ( cidStr == "invalid" )
        {
            return;
        }

        // Add to in-memory disk index
        {
            std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
            diskIndex_.insert( cidStr );
        }

        // Ensure cache directory exists
        namespace fs = std::filesystem;
        std::error_code ec;
        if ( !fs::exists( cacheDir_, ec ) )
        {
            fs::create_directories( cacheDir_, ec );
            if ( ec )
            {
                logger_->warn( "Failed to create cache directory {}: {}", cacheDir_, ec.message() );
                return;
            }
        }

        // Write block to flat file
        auto filePath = cidToFilePath( cidStr );
        std::ofstream file( filePath, std::ios::binary );
        if ( !file.is_open() )
        {
            logger_->warn( "Failed to open cache file for writing: {}", filePath );
            return;
        }
        file.write( blockData.data(), blockData.size() );
        file.close();

        logger_->debug( "Persisted block to disk: {} ({} bytes)", cidStr, blockData.size() );
    }

    void Bitswap::unpersistBlock( const CID &cid )
    {
        if ( cacheDir_.empty() )
        {
            return;
        }

        auto cidStr = cidToString( cid );
        if ( cidStr == "invalid" )
        {
            return;
        }

        auto filePath = cidToFilePath( cidStr );
        std::error_code ec;
        if ( std::filesystem::exists( filePath, ec ) )
        {
            std::filesystem::remove( filePath, ec );
            if ( ec )
            {
                logger_->warn( "Failed to remove cache file {}: {}", filePath, ec.message() );
            }
            else
            {
                logger_->debug( "Removed block from disk: {}", cidStr );
            }
        }

        {
            std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
            diskIndex_.erase( cidStr );
        }
    }

    bool Bitswap::tryLoadFromDisk( const CID &cid )
    {
        if ( cacheDir_.empty() )
        {
            return false;
        }

        auto cidStr = cidToString( cid );
        if ( cidStr == "invalid" )
        {
            return false;
        }

        // Check disk index first (fast path, no I/O)
        {
            std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
            if ( diskIndex_.count( cidStr ) == 0 )
            {
                return false;
            }
        }

        auto filePath = cidToFilePath( cidStr );
        std::ifstream file( filePath, std::ios::binary | std::ios::ate );
        if ( !file.is_open() )
        {
            // File disappeared — remove from index
            std::lock_guard<std::mutex> guard( mutexDiskIndex_ );
            diskIndex_.erase( cidStr );
            logger_->debug( "Cache file missing, removed from index: {}", cidStr );
            return false;
        }

        auto fileSize = static_cast<size_t>( file.tellg() );
        file.seekg( 0, std::ios::beg );

        std::string blockData( fileSize, '\0' );
        file.read( blockData.data(), fileSize );
        file.close();

        // Store in memory
        {
            std::lock_guard<std::mutex> guard( mutexBlockStore_ );
            blockStore_.emplace( cid,
                                 StoredBlock{ blockData,
                                              cid,
                                              std::nullopt,
                                              fileSize,
                                              fileSize,
                                              std::chrono::steady_clock::now() } );
        }

        logger_->debug( "Lazy-loaded block from disk: {} ({} bytes)", cidStr, fileSize );
        return true;
    }
}
