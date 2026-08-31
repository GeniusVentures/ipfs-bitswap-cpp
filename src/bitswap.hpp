#pragma once

#include <memory>
#include <utility>
#include <vector>
#include <optional>
#include <map>
#include <set>
#include <queue>
#include <chrono>
#include <atomic>

#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/base_protocol.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/multi/content_identifier.hpp>
#include <libp2p/outcome/outcome.hpp>
#include <proto/unixfs.pb.h>

#include "logger.hpp"
#include "bitswap_message.hpp"
#include "merkledag_encoder.hpp"

namespace sgns::ipfs_bitswap
{
    namespace merkledag
    {
        struct DecodedLink;
    }

    using CID           = libp2p::multi::ContentIdentifier;
    using BlockCallback = std::function<void( libp2p::outcome::result<std::string> )>;

    // Peer provider tracking
    struct PeerProvider
    {
        libp2p::peer::PeerInfo                peerInfo;
        std::chrono::steady_clock::time_point lastSeen     = std::chrono::steady_clock::now();
        int                                   failureCount = 0;
        bool                                  isReachable  = true;

        PeerProvider( libp2p::peer::PeerInfo peer ) : peerInfo( std::move( peer ) ) {}
    };

    // UnixFS Content structures
    struct UnixFSFile
    {
        std::string                        path;
        std::vector<char>                  content;
        uint64_t                           size;
        std::optional<uint32_t>            mode;
        std::optional<unixfs_pb::UnixTime> mtime;
    };

    struct UnixFSContent
    {
        enum ContentType : uint8_t
        {
            SINGLE_FILE,
            DIRECTORY,
            MULTI_FILE_ARCHIVE
        };

        ContentType                        type;
        std::vector<UnixFSFile>            files;
        std::map<std::string, std::string> metadata;
    };

    using ContentCallback = std::function<void( libp2p::outcome::result<UnixFSContent> )>;
    using PublishCallback = std::function<void( libp2p::outcome::result<CID> )>;

    // Structure for storing blocks in the local block store
    struct StoredBlock
    {
        std::string                           data;
        CID                                   cid;
        std::optional<std::string>            filePath;
        size_t                                size;
        size_t                                contentSize;
        std::chrono::steady_clock::time_point addedTime;
    };

    // Structure for tracking published content
    struct PublishedContent
    {
        std::string                           rootPath;
        CID                                   rootCID;
        std::map<CID, StoredBlock>            blocks;
        UnixFSContent::ContentType            contentType;
        size_t                                totalSize;
        std::chrono::steady_clock::time_point publishedTime;
    };

    enum class BitswapError : uint8_t
    {
        OUTBOUND_STREAM_FAILURE = 1,
        MESSAGE_SENDING_FAILURE,
        REQUEST_TIMEOUT,
        INVALID_UNIXFS_DATA,
        IPLD_DECODE_FAILURE,
        CONTENT_REQUEST_TIMEOUT,
        FILE_NOT_FOUND,
        ENCODING_FAILURE,
        BLOCK_NOT_FOUND,
        NO_PROVIDERS_AVAILABLE
    };

    class BitswapRequestContext : public std::enable_shared_from_this<BitswapRequestContext>
    {
    public:
        BitswapRequestContext( boost::asio::io_context &context );

        void AddCallback( BlockCallback callback );
        void HandleResponse( libp2p::outcome::result<std::string> block );

    private:
        void HandleResponseTimeout();

        /**
         * @mutex mutex_ — Guards callbacks_ list and responseTimer_.
         *   Acquired in AddCallback() (under outer mutexRequestCallbacks_ in messageSent)
         *   and HandleResponse(). HandleResponseTimeout() does NOT acquire mutex_ —
         *   it forwards to HandleResponse which handles its own locking.
         */
        std::mutex                       mutex_;
        std::list<BlockCallback>         callbacks_;
        boost::asio::deadline_timer      responseTimer_;
        boost::posix_time::time_duration responseTimeout_;
    };

    // Context for managing high-level content requests
    class ContentRequestContext
    {
    public:
        ContentRequestContext( boost::asio::io_context &context, CID rootCid );

        struct FileChunk
        {
            std::vector<char>  data;
            size_t             index = 0;
            std::optional<CID> cid;
        };

        struct FileInProgress
        {
            std::string                        path;
            uint64_t                           totalSize = 0;
            std::map<size_t, FileChunk>        chunks;
            size_t                             expectedChunks = 0;
            std::optional<uint32_t>            mode;
            std::optional<unixfs_pb::UnixTime> mtime;
        };

        struct ChunkInfo
        {
            std::optional<CID> parentCid;
            size_t             chunkIndex = 0;
        };

        CID                                   rootCID;
        std::optional<libp2p::peer::PeerInfo> peerInfo;
        ContentCallback                       callback;
        bool                                  useProviders = false;
        std::vector<UnixFSFile>               collectedFiles;
        std::set<CID>                         pendingCIDs;

        std::queue<CID> requestQueue;
        bool            processingQueue = false;
        std::set<CID>   completedCIDs;

        std::map<CID, FileInProgress> filesInProgress;
        std::map<CID, std::string>    cidToPath;
        std::map<CID, ChunkInfo>      chunkToCidIndex;

        boost::asio::deadline_timer      timeout;
        bool                             timedOut = false;
        boost::posix_time::time_duration contentTimeout_;
    };

    /**
     * /bitswap/1.0.0 protocol implementation
     * Allows getting/serving blocks from/to remote peers
     *
     * Concurrency model: 7 mutex domains (mutexRequestCallbacks_, mutexContentRequests_,
     * mutexActiveStreams_, mutexBlockStore_, mutexCacheDir_, mutexDiskIndex_,
     * mutexProviders_) + 2 atomics (maxPeerAttempts_, peerFailureThreshold_).
     * One documented lock ordering: mutexCacheDir_ -> mutexDiskIndex_ (buildDiskIndex).
     * One documented lock ordering: mutexRequestCallbacks_ -> mutexProviders_ (markProviderSuccess).
     * Callbacks posted to io_context (PublishFile/PublishDirectory) or dispatched to
     * io_context (ContentRequestContext callbacks). io_context provides strand
     * serialization for publish and content-request processing.
     * All locks released before async calls and filesystem I/O.
     */
    class Bitswap : public libp2p::protocol::BaseProtocol, public std::enable_shared_from_this<Bitswap>
    {
    public:
        Bitswap( libp2p::Host &host, libp2p::event::Bus &eventBus, std::shared_ptr<boost::asio::io_context> context );
        ~Bitswap() override = default;

        /** Initialize and register protocol handlers (call after shared_ptr construction) */
        void initialize();

        libp2p::peer::Protocol getProtocolId() const override;
        /**
         * @brief Handle an incoming Bitswap stream from a peer.
         *
         * Runs on the libp2p protocol dispatch thread. The server read loop
         * (recurring async reads for wantlist messages) also executes on
         * libp2p I/O threads via the ProtobufMessageReadWriter callback chain.
         *
         * All lock acquisitions in this path occur on libp2p threads, not on
         * the Bitswap io_context.
         */
        void                   handle( libp2p::StreamAndProtocol stream_res ) override;
        void                   start();

        // --- Block requests ---
        void RequestBlock( const libp2p::peer::PeerInfo &pi, const CID &cid, BlockCallback onBlockCallback );
        void RequestBlockWithRetry( const libp2p::peer::PeerInfo &pi,
                                    const CID                    &cid,
                                    BlockCallback                 onBlockCallback,
                                    int                           retryCount );

        // --- Content requests (UnixFS tree traversal) ---
        void RequestContent( const libp2p::peer::PeerInfo &pi, const CID &cid, ContentCallback onContentCallback );
        void RequestContent( const CID &cid, ContentCallback onContentCallback );

        // --- Provider management ---
        void                      AddProvider( const CID &cid, libp2p::peer::PeerInfo peerInfo );
        void                      AddProviders( const CID &cid, const std::vector<libp2p::peer::PeerInfo> &peerInfos );
        void                      RemoveProvider( const CID &cid, const libp2p::peer::PeerId &peerId );
        std::vector<PeerProvider> GetProviders( const CID &cid ) const;
        void                      ClearProviders( const CID &cid );
        void                      SetMaxPeerAttempts( size_t maxPeers );
        void                      SetPeerFailureThreshold( int threshold );
        size_t                    GetTotalProviderCount() const;
        std::map<std::string, std::vector<std::string>> GetProviderDebugInfo() const;

        // --- Publishing ---
        /** @note Callback fires on Bitswap's internal io_context. Consumer callbacks must not block the io_context thread. */
        void PublishFile( const std::string &filePath, PublishCallback onPublishCallback );
        /** @note Callback fires on Bitswap's internal io_context. Consumer callbacks must not block the io_context thread. */
        void PublishDirectory( const std::string &directoryPath, PublishCallback onPublishCallback );
        void PublishData( const std::vector<uint8_t> &data, PublishCallback onPublishCallback );

        // --- Local block store ---
        bool                                 HasBlock( const CID &cid ) const;
        libp2p::outcome::result<std::string> GetBlock( const CID &cid );
        bool                                 UnpublishContent( const CID &rootCid );
        std::vector<PublishedContent>        ListPublishedContent() const;

        // --- Disk persistence (lazy-load) ---
        /** @note Sets cache directory under mutex (`mutexCacheDir_`) protection. Call once before starting async operations for deterministic behavior. */
        void setCacheDir( const std::string &dir );
        /** @note Returns a copy of the cache directory under mutex (`mutexCacheDir_`) protection. Thread-safe for concurrent calls. */
        std::string getCacheDir() const;
        void buildDiskIndex();
        void persistBlock( const CID &cid, const std::string &blockData );
        void unpersistBlock( const CID &cid );

    private:
        void onNewConnection( const std::weak_ptr<libp2p::connection::CapableConnection> &conn );
        void writeBitswapMessageToStream( std::shared_ptr<libp2p::connection::Stream> stream,
                                          const CID                                  &cid,
                                          BlockCallback                               onBlockCallback );
        void messageSent( libp2p::outcome::result<size_t>             writtenBytes,
                          std::shared_ptr<libp2p::connection::Stream> stream,
                          const CID                                  &cid,
                          BlockCallback                               onBlockCallback );
        void logStreamState( const std::string_view &message, libp2p::connection::Stream &stream );

        // Block response processing (shared by handle() and messageSent())
        void processReceivedBlocks( const BitswapMessage                              &msg,
                                    const std::shared_ptr<libp2p::connection::Stream> &stream );

        // UnixFS content processing
        void          processUnixFSBlock( std::shared_ptr<ContentRequestContext> ctx,
                                          const CID                             &cid,
                                          const std::string                     &blockData,
                                          const std::string                     &path = "" );
        void          handleFileBlock( std::shared_ptr<ContentRequestContext> ctx,
                                       const CID                             &cid,
                                       const unixfs_pb::Data                 &unixfsData,
                                       const std::vector<merkledag::DecodedLink> &links,
                                       const std::string                     &path = "" );
        void          handleFileChunk( std::shared_ptr<ContentRequestContext> ctx,
                                       const CID                             &chunkCid,
                                       const std::string                     &chunkData,
                                       size_t                                 chunkIndex,
                                       const CID                             &parentCid );
        void          assembleCompleteFile( std::shared_ptr<ContentRequestContext>       ctx,
                                            const CID                                   &fileCid,
                                            const ContentRequestContext::FileInProgress &fileProgress );
        void          handleDirectoryBlock( std::shared_ptr<ContentRequestContext> ctx,
                                            const std::vector<merkledag::DecodedLink> &links,
                                            const std::string                     &basePath = "" );
        void          checkContentRequestComplete( std::shared_ptr<ContentRequestContext> ctx );
        UnixFSContent assembleContent( std::shared_ptr<ContentRequestContext> ctx );
        void          processRequestQueue( std::shared_ptr<ContentRequestContext> ctx );

        // Content request setup helper (shared by both RequestContent overloads)
        std::shared_ptr<ContentRequestContext> setupContentRequest( const CID      &cid,
                                                                    ContentCallback onContentCallback,
                                                                    bool            useProviders );
        void                                   failContentRequest( ContentRequestContext &ctx, BitswapError error );

        // Queue result handler (shared callback logic in processRequestQueue)
        void handleQueuedBlockResult( std::shared_ptr<ContentRequestContext> ctx,
                                      const CID                             &nextCid,
                                      libp2p::outcome::result<std::string>   result );

        // Server-side encoding and storage
        CID encodeAndStoreFile( const std::string &filePath );
        CID encodeChunkedFile( const std::vector<uint8_t> &content, const std::string &filePath );
        CID encodeAndStoreDirectory( const std::string &directoryPath );
        CID encodeAndStoreData( const std::vector<uint8_t> &data,
                                unixfs_pb::Data::DataType   type = unixfs_pb::Data::Raw );

        // IPLD node creation - unified helper
        CID encodeAndStoreMerkledagNode( const std::string &unixfsData,
                                         const std::vector<merkledag::Link> &links );
        CID encodeAndStoreMerkledagNode( const std::string                &unixfsData,
                                         const std::vector<merkledag::Link> &links,
                                         size_t                            contentSize );

        void storeBlock( const CID         &cid,
                         const std::string &blockData,
                         const std::string &originalPath = "",
                         size_t             contentSize  = 0 );
        void handleWantlistRequest( const CID &wantedCid, std::shared_ptr<libp2p::connection::Stream> stream );
        void sendBlockResponse( const CID                                  &cid,
                                const std::string                          &blockData,
                                std::shared_ptr<libp2p::connection::Stream> stream );

        // Disk persistence helpers
        bool tryLoadFromDisk( const CID &cid );
        std::string cidToFilePath( const std::string &cidStr ) const;

        // Provider management helpers
        libp2p::peer::PeerInfo selectBestProvider( const CID &cid );
        void                   markProviderFailure( const CID &cid, const libp2p::peer::PeerId &peerId );
        void                   markProviderSuccess( const CID &cid, const libp2p::peer::PeerId &peerId );
        PeerProvider          *findProvider( const CID &cid, const libp2p::peer::PeerId &peerId );

        // Internal request methods with provider failover
        void requestBlockWithProviders( const CID &cid, BlockCallback onBlockCallback, int attemptCount = 0 );
        void requestBlockWithProvidersFromRoot( const CID    &rootCid,
                                                const CID    &targetCid,
                                                BlockCallback onBlockCallback,
                                                int           attemptCount = 0 );

        libp2p::Host         &host_;
        libp2p::event::Bus   &bus_;
        libp2p::event::Handle sub_;

        std::shared_ptr<boost::asio::io_context> context_;
        bool                                     started_ = false;

        /**
         * @name Request Callback Synchronization
         *
         * @mutex mutexRequestCallbacks_ — Guards requestContexts_ map.
         *   Lock ordering: mutexRequestCallbacks_ -> mutexProviders_ (via markProviderSuccess).
         *   Held on: libp2p I/O thread (processReceivedBlocks, messageSent).
         *   Callbacks NOT invoked under this lock (C-7 fix).
         */
        ///@{
        mutable std::mutex                                    mutexRequestCallbacks_;
        std::map<CID, std::shared_ptr<BitswapRequestContext>> requestContexts_;
        ///@}

        /**
         * @name Content Request Synchronization
         *
         * @mutex mutexContentRequests_ — Guards contentRequests_ map.
         *   Held on: caller thread and io_context (timeout handler).
         *   NOT nested with other mutexes. Callbacks invoked under this lock (timeout path;
         *   re-entrancy risk noted — content callback should not re-enter Bitswap's request APIs).
         */
        ///@{
        mutable std::mutex                                    mutexContentRequests_;
        std::map<CID, std::shared_ptr<ContentRequestContext>> contentRequests_;
        ///@}

        /**
         * @name Active Stream Synchronization
         *
         * @mutex mutexActiveStreams_ — Guards activeStreams_ map.
         *   Held on: caller thread, io_context (retry timer), libp2p callback (newStream result).
         *   Released before all async calls (writeBitswapMessageToStream, host_.newStream).
         *   NOT nested with other mutexes. Stream cached via shared_ptr for lifetime safety.
         */
        ///@{
        mutable std::mutex                                                          mutexActiveStreams_;
        std::map<libp2p::peer::PeerId, std::shared_ptr<libp2p::connection::Stream>> activeStreams_;
        ///@}

        /**
         * @name Block Store Synchronization
         *
         * @mutex mutexBlockStore_ — Guards blockStore_ and publishedContent_ maps.
         *   Held on: caller thread, libp2p thread (handleWantlistRequest), io_context (publish work).
         *   NOT nested with any other mutex. Always released before async calls or filesystem I/O.
         */
        ///@{
        mutable std::mutex              mutexBlockStore_;
        std::map<CID, StoredBlock>      blockStore_;
        std::map<CID, PublishedContent> publishedContent_;
        ///@}

        /**
         * @name Cache Directory Synchronization
         *
         * @mutex mutexCacheDir_ — Guards cacheDir_ string.
         *   Lock ordering: mutexCacheDir_ -> mutexDiskIndex_ (via buildDiskIndex).
         *   Held for minimum scope — value copied to local before any filesystem I/O.
         *   Held on: caller thread, main init thread (buildDiskIndex).
         */
        ///@{
        std::string                cacheDir_;
        mutable std::mutex         mutexCacheDir_;
        ///@}
        /**
         * @name Disk Index Synchronization
         *
         * @mutex mutexDiskIndex_ — Guards diskIndex_ set.
         *   Held on: main init thread (buildDiskIndex), caller thread (persistBlock, unpersistBlock),
         *            libp2p/consumer thread (tryLoadFromDisk, HasBlock).
         *   NOT nested with other mutexes except via mutexCacheDir_ -> mutexDiskIndex_ in buildDiskIndex.
         *   buildDiskIndex() clear-then-populate creates a transitory empty-index window — benign.
         */
        ///@{
        mutable std::mutex         mutexDiskIndex_;
        std::set<std::string>      diskIndex_;
        ///@}

        /**
         * @name Provider Synchronization
         *
         * @mutex mutexProviders_ — Guards providers_ map.
         *   Lock ordering: acquired inside mutexRequestCallbacks_ via markProviderSuccess.
         *   Held on: caller thread, libp2p thread (markProviderSuccess).
         *   findProvider() returns raw PeerProvider* valid only under lock.
         *
         * @atomic maxPeerAttempts_ — Max provider selection attempts (C-2 fix).
         *   Written by SetMaxPeerAttempts (release ordering).
         *   Read under mutexProviders_ in selectBestProvider (relaxed ordering sufficient).
         *   Read without lock in requestBlockWithProviders* (acquire ordering).
         *
         * @atomic peerFailureThreshold_ — Failure count before marking provider unreachable (C-2 fix).
         *   Written by SetPeerFailureThreshold (release ordering).
         *   Read under mutexProviders_ in selectBestProvider/markProviderFailure (relaxed ordering).
         */
        ///@{
        mutable std::mutex                       mutexProviders_;
        std::map<CID, std::vector<PeerProvider>> providers_;
        std::atomic<size_t>                      maxPeerAttempts_{3};
        std::atomic<int>                         peerFailureThreshold_{3};
        ///@}

        Logger logger_ = createLogger( "Bitswap" );
    };
}

OUTCOME_HPP_DECLARE_ERROR_2( sgns::ipfs_bitswap, BitswapError );
