#pragma once

#include "logger.hpp"

#include <memory>
#include <vector>
#include <optional>
#include <map>
#include <set>
#include <queue>
#include <chrono>

#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/base_protocol.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/multi/content_identifier.hpp>
#include <libp2p/outcome/outcome.hpp>
#include <proto/unixfs.pb.h>

namespace sgns::ipfs_bitswap
{
    // Forward declarations
    class MerkledagDecoder;

    typedef libp2p::multi::ContentIdentifier                            CID;
    typedef std::function<void( libp2p::outcome::result<std::string> )> BlockCallback;

    // Peer provider tracking
    struct PeerProvider
    {
        libp2p::peer::PeerInfo                peerInfo;
        std::chrono::steady_clock::time_point lastSeen;
        int                                   failureCount = 0;
        bool                                  isReachable  = true;

        PeerProvider( const libp2p::peer::PeerInfo &peer ) :
            peerInfo( peer ), lastSeen( std::chrono::steady_clock::now() )
        {
        }
    };

    // UnixFS Content structures
    struct UnixFSFile
    {
        std::string                        path;    // e.g., "", "subdir/file.txt"
        std::vector<char>                  content; // actual file data
        uint64_t                           size;
        std::optional<uint32_t>            mode;
        std::optional<unixfs_pb::UnixTime> mtime;
    };

    struct UnixFSContent
    {
        enum ContentType
        {
            SINGLE_FILE,
            DIRECTORY,
            MULTI_FILE_ARCHIVE
        };

        ContentType                        type;
        std::vector<UnixFSFile>            files;
        std::map<std::string, std::string> metadata;
    };

    typedef std::function<void( libp2p::outcome::result<UnixFSContent> )> ContentCallback;
    typedef std::function<void( libp2p::outcome::result<CID> )>           PublishCallback;

    // Structure for storing blocks in the local block store
    struct StoredBlock
    {
        std::string                data;        // Raw block data
        CID                        cid;         // Block's content identifier
        std::optional<std::string> filePath;    // Original file path (if from file)
        size_t                     size;        // IPLD block size (serialized size)
        size_t                     contentSize; // Total content size (for files, this is the actual file size)
        std::chrono::steady_clock::time_point addedTime;
    };

    // Structure for tracking published content
    struct PublishedContent
    {
        std::string                           rootPath; // Root directory or file path
        CID                                   rootCID;  // Root CID of the content
        std::map<CID, StoredBlock>            blocks;   // All blocks that make up this content
        UnixFSContent::ContentType            contentType;
        size_t                                totalSize;
        std::chrono::steady_clock::time_point publishedTime;
    };

    enum class BitswapError
    {
        OUTBOUND_STREAM_FAILURE = 1,
        MESSAGE_SENDING_FAILURE,
        REQUEST_TIMEOUT,
        INVALID_UNIXFS_DATA,
        IPLD_DECODE_FAILURE,
        CONTENT_REQUEST_TIMEOUT,
        FILE_NOT_FOUND,
        ENCODING_FAILURE,
        BLOCK_NOT_FOUND
    };

    class BitswapRequestContext
    {
    public:
        BitswapRequestContext( boost::asio::io_context &context, const CID &cid );

        void AddCallback( BlockCallback callback );
        void HandleResponse( libp2p::outcome::result<std::string> block );

    private:
        void HandleResponseTimeout();

        std::list<BlockCallback> callbacks_;

        boost::asio::deadline_timer      responseTimer_;
        boost::posix_time::time_duration responseTimeout_;
    };

    // Context for managing high-level content requests
    class ContentRequestContext
    {
    public:
        ContentRequestContext( boost::asio::io_context &context, const CID &rootCid );

        // Enhanced chunk tracking
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
            std::map<size_t, FileChunk>        chunks; // chunk index -> chunk data
            size_t                             expectedChunks = 0;
            std::optional<uint32_t>            mode;
            std::optional<unixfs_pb::UnixTime> mtime;
        };

        struct ChunkInfo
        {
            std::optional<CID> parentCid;
            size_t             chunkIndex = 0;

            ChunkInfo() = default;

            ChunkInfo( const CID &parent, size_t index ) : parentCid( parent ), chunkIndex( index ) {}
        };

        CID rootCID;
        std::optional<libp2p::peer::PeerInfo>
                        peerInfo; // Store peer info for additional requests (optional for provider-based requests)
        ContentCallback callback;
        bool            useProviders = false; // Whether to use provider system for sub-requests
        std::vector<UnixFSFile> collectedFiles;
        std::set<CID>           pendingCIDs;

        // Request queue for sequential processing
        std::queue<CID> requestQueue;
        bool            processingQueue = false;
        std::set<CID>   completedCIDs;

        std::map<CID, FileInProgress> filesInProgress; // CID -> file being assembled
        std::map<CID, std::string>    cidToPath;       // Track path for each CID
        std::map<CID, ChunkInfo>      chunkToCidIndex; // chunk CID -> parent file info

        boost::asio::deadline_timer      timeout;
        bool                             timedOut = false;
        boost::posix_time::time_duration contentTimeout_;

    private:
    };

    /**
    * /bitswap/1.0.0 protocol implementation
    * It allows to get a block from remote peer
    */
    class Bitswap : public libp2p::protocol::BaseProtocol, public std::enable_shared_from_this<Bitswap>
    {
    public:
        /**
        * Creates a bitswap protocol instance
        * @param host - local host
        * @param eventBus - bus to subscribe to network events
        */
        Bitswap( libp2p::Host &host, libp2p::event::Bus &eventBus, std::shared_ptr<boost::asio::io_context> context );

        /**
        * Initialize and register protocol handlers (call after shared_ptr construction)
        */
        void initialize();

        ~Bitswap() override = default;

        /**
        * @return bitswap protocol identifier
        */
        libp2p::peer::Protocol getProtocolId() const override;

        /**
        * In Bitswap, handle bitswap request
        */
        void handle( StreamResult stream_res ) override;

        /**
        * Start accepting bitswap requests
        */
        void start();

        /**
        * Requests a block from a remote peer
        * @param pi - remote peer info
        * @param cid - block content identifier
        * @param onBlockCallback - a callback that is called when data is received
        */
        void RequestBlock( const libp2p::peer::PeerInfo &pi, const CID &cid, BlockCallback onBlockCallback );

        /**
        * Internal retry logic for RequestBlock
        */
        void RequestBlockWithRetry( const libp2p::peer::PeerInfo &pi,
                                    const CID                    &cid,
                                    BlockCallback                 onBlockCallback,
                                    int                           retryCount );

        /**
        * Requests complete UnixFS content from a remote peer
        * Automatically handles tree traversal, chunk reassembly, and directory structures
        * @param pi - remote peer info
        * @param cid - root content identifier
        * @param onContentCallback - callback with structured content when complete
        */
        void RequestContent( const libp2p::peer::PeerInfo &pi, const CID &cid, ContentCallback onContentCallback );

        /**
        * Requests complete UnixFS content using known providers for the CID
        * Automatically selects from available peers and handles failover
        * @param cid - root content identifier
        * @param onContentCallback - callback with structured content when complete
        */
        void RequestContent( const CID &cid, ContentCallback onContentCallback );

        /**
        * Adds a peer as a provider for a specific CID
        * @param cid - content identifier
        * @param peerInfo - peer that provides this content
        */
        void AddProvider( const CID &cid, const libp2p::peer::PeerInfo &peerInfo );

        /**
        * Removes a peer as a provider for a specific CID
        * @param cid - content identifier  
        * @param peerId - peer to remove
        */
        void RemoveProvider( const CID &cid, const libp2p::peer::PeerId &peerId );

        /**
        * Gets all known providers for a CID
        * @param cid - content identifier
        * @return vector of peer providers
        */
        std::vector<PeerProvider> GetProviders( const CID &cid ) const;

        /**
        * Clears all providers for a CID
        * @param cid - content identifier
        */
        void ClearProviders( const CID &cid );

        /**
        * Sets the maximum number of peers to try per request
        * @param maxPeers - maximum peers to attempt (default: 3)
        */
        void SetMaxPeerAttempts( size_t maxPeers );

        /**
        * Sets the failure threshold before marking a peer as unreachable
        * @param threshold - number of failures (default: 3)
        */
        void SetPeerFailureThreshold( int threshold );

        /**
        * Adds multiple providers for a CID (useful when getting results from DHT)
        * @param cid - content identifier
        * @param peerInfos - list of peers that provide this content
        */
        void AddProviders( const CID &cid, const std::vector<libp2p::peer::PeerInfo> &peerInfos );

        /**
        * Gets the total number of known providers across all CIDs
        * @return total provider count
        */
        size_t GetTotalProviderCount() const;

        /**
        * Gets debug information about all providers
        * @return map of CID strings to provider info strings
        */
        std::map<std::string, std::vector<std::string>> GetProviderDebugInfo() const;

        /**
        * Publishes a single file to the bitswap network
        * @param filePath - path to the file to publish
        * @param onPublishCallback - callback with root CID when publishing is complete
        */
        void PublishFile( const std::string &filePath, PublishCallback onPublishCallback );

        /**
        * Publishes a directory and its contents to the bitswap network
        * @param directoryPath - path to the directory to publish
        * @param onPublishCallback - callback with root CID when publishing is complete
        */
        void PublishDirectory( const std::string &directoryPath, PublishCallback onPublishCallback );

        /**
        * Publishes raw data as a block to the bitswap network
        * @param data - raw data to publish
        * @param onPublishCallback - callback with CID when publishing is complete
        */
        void PublishData( const std::vector<uint8_t> &data, PublishCallback onPublishCallback );

        /**
        * Checks if a block with the given CID is available locally
        * @param cid - content identifier to check
        * @return true if block is available locally
        */
        bool HasBlock( const CID &cid ) const;

        /**
        * Gets a block from local storage
        * @param cid - content identifier
        * @return block data if found, error otherwise
        */
        libp2p::outcome::result<std::string> GetBlock( const CID &cid ) const;

        /**
        * Removes published content from the local store
        * @param rootCid - root CID of content to unpublish
        * @return true if content was found and removed
        */
        bool UnpublishContent( const CID &rootCid );

        /**
        * Lists all published content
        * @return vector of published content info
        */
        std::vector<PublishedContent> ListPublishedContent() const;

    private:
        /**
        * Handler for new connections, established by or with our host
        * @param conn - new connection
        */
        void onNewConnection( const std::weak_ptr<libp2p::connection::CapableConnection> &conn );

        /**
        * Write a bitswap message containing a block request to stream
        * @param stream - outbound stream
        * @param cid - requested block content identifier
        */
        void writeBitswapMessageToStream( std::shared_ptr<libp2p::connection::Stream> stream,
                                          const CID                                  &cid,
                                          BlockCallback                               onBlockCallback );

        void messageSent( libp2p::outcome::result<size_t>             writtenBytes,
                          std::shared_ptr<libp2p::connection::Stream> stream,
                          const CID                                  &cid,
                          BlockCallback                               onBlockCallback );

        void logStreamState( const std::string_view &message, libp2p::connection::Stream &stream );
        void handleResponseTimeout( const CID &cid );

        // UnixFS content processing methods
        void          processUnixFSBlock( std::shared_ptr<ContentRequestContext> ctx,
                                          const CID                             &cid,
                                          const std::string                     &blockData,
                                          const std::string                     &path = "" );
        void          handleFileBlock( std::shared_ptr<ContentRequestContext> ctx,
                                       const CID                             &cid,
                                       const unixfs_pb::Data                 &unixfsData,
                                       const MerkledagDecoder                &decoder,
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
                                            const CID                             &cid,
                                            const unixfs_pb::Data                 &unixfsData,
                                            const MerkledagDecoder                &decoder,
                                            const std::string                     &basePath = "" );
        void          checkContentRequestComplete( std::shared_ptr<ContentRequestContext> ctx );
        UnixFSContent assembleContent( std::shared_ptr<ContentRequestContext> ctx );
        void          processRequestQueue( std::shared_ptr<ContentRequestContext> ctx );

        libp2p::Host         &host_;
        libp2p::event::Bus   &bus_;
        libp2p::event::Handle sub_; // will unsubscribe during destruction by itself

        std::shared_ptr<boost::asio::io_context> context_;
        bool                                     started_ = false;

        mutable std::mutex                                    mutexRequestCallbacks_;
        std::map<CID, std::shared_ptr<BitswapRequestContext>> requestContexts_;

        mutable std::mutex                                    mutexContentRequests_;
        std::map<CID, std::shared_ptr<ContentRequestContext>> contentRequests_;

        // Stream management for reusing active streams
        mutable std::mutex                                                          mutexActiveStreams_;
        std::map<libp2p::peer::PeerId, std::shared_ptr<libp2p::connection::Stream>> activeStreams_;

        // Server-side storage for published content
        mutable std::mutex              mutexBlockStore_;
        std::map<CID, StoredBlock>      blockStore_;       // CID -> block data
        std::map<CID, PublishedContent> publishedContent_; // Root CID -> published content info

        // Peer provider tracking
        mutable std::mutex                       mutexProviders_;
        std::map<CID, std::vector<PeerProvider>> providers_; // CID -> list of providers
        size_t                                   maxPeerAttempts_      = 3;
        int                                      peerFailureThreshold_ = 3;

        Logger logger_ = createLogger( "Bitswap" );

    private:
        // Server-side encoding and storage methods
        CID         encodeAndStoreFile( const std::string &filePath );
        CID         encodeChunkedFile( const std::vector<uint8_t> &content, const std::string &filePath );
        CID         encodeAndStoreDirectory( const std::string &directoryPath );
        CID         encodeAndStoreData( const std::vector<uint8_t> &data,
                                        unixfs_pb::Data::DataType   type = unixfs_pb::Data::Raw );
        std::string createUnixFSData( const std::vector<uint8_t> &content,
                                      unixfs_pb::Data::DataType   type,
                                      uint64_t                    filesize = 0,
                                      const std::vector<CID>     &links    = {} );
        CID         createIPLDNode( const std::string &unixfsData, const std::map<std::string, CID> &links = {} );
        CID         createIPLDNode( const std::string &unixfsData, const std::vector<CID> &orderedChunkCIDs );
        CID         createIPLDNodeWithContentSize( const std::string      &unixfsData,
                                                   const std::vector<CID> &orderedChunkCIDs,
                                                   size_t                  totalContentSize );
        CID createIPLDNodeAndStoreUnixFS( const std::string &unixfsData, const std::map<std::string, CID> &links = {} );
        CID createIPLDNodeAndStoreRawData( const std::vector<uint8_t> &rawData );
        void        analyzeIPLDStructure( const std::vector<uint8_t> &data, const std::string &label );
        std::string bytesToHex( const std::vector<uint8_t> &bytes );
        void        storeBlock( const CID &cid, const std::string &blockData, const std::string &originalPath = "" );
        void        storeBlock( const CID         &cid,
                                const std::string &blockData,
                                const std::string &originalPath,
                                size_t             contentSize );
        void        handleWantlistRequest( const CID &wantedCid, std::shared_ptr<libp2p::connection::Stream> stream );
        void        sendBlockResponse( const CID                                  &cid,
                                       const std::string                          &blockData,
                                       std::shared_ptr<libp2p::connection::Stream> stream );

        // Provider management helpers
        libp2p::peer::PeerInfo selectBestProvider( const CID &cid );
        void                   markProviderFailure( const CID &cid, const libp2p::peer::PeerId &peerId );
        void                   markProviderSuccess( const CID &cid, const libp2p::peer::PeerId &peerId );
        void                   cleanupStaleProviders();

        // Internal request methods with provider failover
        void requestBlockWithProviders( const CID &cid, BlockCallback onBlockCallback, int attemptCount = 0 );
        void requestBlockWithProvidersFromRoot( const CID    &rootCid,
                                                const CID    &targetCid,
                                                BlockCallback onBlockCallback,
                                                int           attemptCount = 0 );
    };
} // ipfs_bitswap

OUTCOME_HPP_DECLARE_ERROR_2( sgns::ipfs_bitswap, BitswapError );
