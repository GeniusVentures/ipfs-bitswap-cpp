#ifndef IPFS_BITSWAP_HPP
#define IPFS_BITSWAP_HPP

#include "logger.hpp"

#include <memory>
#include <vector>

#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/base_protocol.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/multi/content_identifier.hpp>
#include <libp2p/outcome/outcome.hpp>

namespace sgns::ipfs_bitswap 
{
    typedef libp2p::multi::ContentIdentifier CID;
    typedef std::function<void(libp2p::outcome::result<std::string>)> BlockCallback;

    enum class BitswapError
    {
        OUTBOUND_STREAM_FAILURE = 1,
        MESSAGE_SENDING_FAILURE,
        REQUEST_TIMEOUT
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

        libp2p::Host& host_;
        libp2p::event::Bus& bus_;
        libp2p::event::Handle sub_;  // will unsubscribe during destruction by itself

        std::shared_ptr<boost::asio::io_context> context_;
        bool started_ = false;

        mutable std::mutex mutexRequestCallbacks_;
        std::map<CID, std::shared_ptr<BitswapRequestContext>> requestContexts_;

        Logger logger_ = createLogger("Bitswap");
    };
}  // ipfs_bitswap

OUTCOME_HPP_DECLARE_ERROR_2(sgns::ipfs_bitswap, BitswapError);

#endif  // IPFS_BITSWAP_HPP
