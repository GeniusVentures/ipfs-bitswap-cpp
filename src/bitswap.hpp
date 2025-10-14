#ifndef IPFS_BITSWAP_HPP
#define IPFS_BITSWAP_HPP

#include "logger.hpp"

#include <memory>
#include <vector>
#include <optional>
#include <map>
#include <set>

#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/base_protocol.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/multi/content_identifier.hpp>
#include <libp2p/outcome/outcome.hpp>
#include <proto/unixfs.pb.h>
#include <ipfs_lite/ipld/impl/ipld_node_decoder_pb.hpp>

namespace sgns::ipfs_bitswap 
{
    typedef libp2p::multi::ContentIdentifier CID;
    typedef std::function<void(libp2p::outcome::result<std::string>)> BlockCallback;

    // UnixFS Content structures
    struct UnixFSFile {
        std::string path;           // e.g., "", "subdir/file.txt" 
        std::vector<char> content;  // actual file data
        uint64_t size;
        std::optional<uint32_t> mode;
        std::optional<unixfs_pb::UnixTime> mtime;
    };

    struct UnixFSContent {
        enum ContentType {
            SINGLE_FILE,
            DIRECTORY,
            MULTI_FILE_ARCHIVE
        };
        
        ContentType type;
        std::vector<UnixFSFile> files;
        std::map<std::string, std::string> metadata;
    };

    typedef std::function<void(libp2p::outcome::result<UnixFSContent>)> ContentCallback;

    enum class BitswapError
    {
        OUTBOUND_STREAM_FAILURE = 1,
        MESSAGE_SENDING_FAILURE,
        REQUEST_TIMEOUT,
        INVALID_UNIXFS_DATA,
        IPLD_DECODE_FAILURE,
        CONTENT_REQUEST_TIMEOUT
    };

    class BitswapRequestContext
    {
    public:
        BitswapRequestContext(boost::asio::io_context& context, const CID& cid);

        void AddCallback(BlockCallback callback);
        void HandleResponse(libp2p::outcome::result<std::string> block);
    private:
        void HandleResponseTimeout();

        std::list<BlockCallback> callbacks_;
            
        boost::asio::deadline_timer responseTimer_;
        boost::posix_time::time_duration responseTimeout_;
    };

    // Context for managing high-level content requests
    class ContentRequestContext
    {
    public:
        ContentRequestContext(boost::asio::io_context& context, const CID& rootCid);

        CID rootCID;
        libp2p::peer::PeerInfo peerInfo;  // Store peer info for additional requests
        ContentCallback callback;
        std::vector<UnixFSFile> collectedFiles;
        std::set<CID> pendingCIDs;
        std::set<CID> completedCIDs;
        std::map<std::string, std::vector<char>> fileChunks; // path -> ordered chunks
        
        boost::asio::deadline_timer timeout;
        bool timedOut = false;

    private:
        boost::posix_time::time_duration contentTimeout_;
    };
    /**
    * /bitswap/1.0.0 protocol implementation
    * It allows to get a block from remote peer
    */
    class Bitswap : public libp2p::protocol::BaseProtocol,
        public std::enable_shared_from_this<Bitswap> 
    {
    public:
        /**
        * Creates a bitswap protocol instance
        * @param host - local host
        * @param eventBus - bus to subscribe to network events
        */
        Bitswap(
            libp2p::Host& host,
            libp2p::event::Bus& eventBus,
            std::shared_ptr<boost::asio::io_context> context);

        ~Bitswap() override = default;

        /**
        * @return bitswap protocol identifier
        */
        libp2p::peer::Protocol getProtocolId() const override;

        /**
        * In Bitswap, handle bitswap request
        */
        void handle(StreamResult stream_res) override;

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
        void RequestBlock(
            const libp2p::peer::PeerInfo& pi,
            const CID& cid,
            BlockCallback onBlockCallback);

        /**
        * Requests complete UnixFS content from a remote peer
        * Automatically handles tree traversal, chunk reassembly, and directory structures
        * @param pi - remote peer info
        * @param cid - root content identifier
        * @param onContentCallback - callback with structured content when complete
        */
        void RequestContent(
            const libp2p::peer::PeerInfo& pi,
            const CID& cid,
            ContentCallback onContentCallback);
    private:
        /**
        * Handler for new connections, established by or with our host
        * @param conn - new connection
        */
        void onNewConnection(
            const std::weak_ptr<libp2p::connection::CapableConnection>& conn);

        /**
        * Write a bitswap message containing a block request to stream
        * @param stream - outbound stream
        * @param cid - requested block content identifier
        */
        void writeBitswapMessageToStream(
            std::shared_ptr<libp2p::connection::Stream> stream,
            const CID& cid,
            BlockCallback onBlockCallback);

        void messageSent(
            libp2p::outcome::result<size_t> writtenBytes, 
            std::shared_ptr<libp2p::connection::Stream> stream,
            const CID& cid,
            BlockCallback onBlockCallback);

        void logStreamState(const std::string_view& message, libp2p::connection::Stream& stream);
        void handleResponseTimeout(const CID& cid);

        // UnixFS content processing methods
        void processUnixFSBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const std::string& blockData);
        void handleFileBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const unixfs_pb::Data& unixfsData, const ipfs_lite::ipld::IPLDNodeDecoderPB& decoder, const std::string& path = "");
        void handleDirectoryBlock(std::shared_ptr<ContentRequestContext> ctx, const CID& cid, const unixfs_pb::Data& unixfsData, const ipfs_lite::ipld::IPLDNodeDecoderPB& decoder, const std::string& basePath = "");
        void checkContentRequestComplete(std::shared_ptr<ContentRequestContext> ctx);
        UnixFSContent assembleContent(std::shared_ptr<ContentRequestContext> ctx);

        libp2p::Host& host_;
        libp2p::event::Bus& bus_;
        libp2p::event::Handle sub_;  // will unsubscribe during destruction by itself

        std::shared_ptr<boost::asio::io_context> context_;
        bool started_ = false;

        mutable std::mutex mutexRequestCallbacks_;
        std::map<CID, std::shared_ptr<BitswapRequestContext>> requestContexts_;
        
        mutable std::mutex mutexContentRequests_;
        std::map<CID, std::shared_ptr<ContentRequestContext>> contentRequests_;

        Logger logger_ = createLogger("Bitswap");
    };
}  // ipfs_bitswap

OUTCOME_HPP_DECLARE_ERROR_2(sgns::ipfs_bitswap, BitswapError);

#endif  // IPFS_BITSWAP_HPP
