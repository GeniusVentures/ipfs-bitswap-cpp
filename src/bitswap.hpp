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
    /**
    * /bitswap/1.0.0 protocol implementation
    * It allows to get a block from remote peer
    */
    class Bitswap : public libp2p::protocol::BaseProtocol,
        public std::enable_shared_from_this<Bitswap> 
    {
    public:
        typedef libp2p::multi::ContentIdentifier CID;
        typedef std::function<void(libp2p::outcome::result<std::string>)> BlockCallback;

        enum class BitswapError
        {
            OUTBOUND_STREAM_FAILURE = 1,
            MESSAGE_SENDING_FAILURE,
        };

        /**
        * Creates a bitswap protocol instance
        * @param host - local host
        * @param eventBus - bus to subscribe to network events
        */
        Bitswap(libp2p::Host& host,
                libp2p::event::Bus& eventBus);

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
        * @param peer - remote peer id
        * @param address - remote peer address
        * @param cid - block content identifier
        * @param onBlockCallback - a callback that is called when data is received
        */
        void RequestBlock(
            const libp2p::peer::PeerId& peer,
            boost::optional<libp2p::multi::Multiaddress> address,
            const CID& cid,
            BlockCallback onBlockCallback);

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
        * Sends a bitswap message containing a block request to stream
        * @param stream - outbound stream
        * @param cid - requested block content identifier
        */
        void sendRequest(
            std::shared_ptr<libp2p::connection::Stream> stream,
            const CID& cid,
            BlockCallback onBlockCallback);

        void messageSent(
            libp2p::outcome::result<size_t> writtenBytes, 
            std::shared_ptr<libp2p::connection::Stream> stream,
            const CID& cid,
            BlockCallback onBlockCallback);

        libp2p::Host& host_;
        libp2p::event::Bus& bus_;
        libp2p::event::Handle sub_;  // will unsubscribe during destruction by itself

        bool started_ = false;

        mutable std::mutex mutexRequestCallbacks_;
        std::map<CID, std::list<BlockCallback>> requestCallbacks_;

        Logger logger_ = createLogger("Bitswap");
    };
}  // ipfs_bitswap

OUTCOME_HPP_DECLARE_ERROR_2(sgns::ipfs_bitswap, Bitswap::BitswapError);

#endif  // IPFS_BITSWAP_HPP
