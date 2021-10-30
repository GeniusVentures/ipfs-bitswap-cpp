#ifndef IPFS_BITSWAP_HPP
#define IPFS_BITSWAP_HPP

#include "logger.hpp"

#include <memory>
#include <vector>

#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/base_protocol.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/multi/content_identifier.hpp>

namespace sgns::ipfs_bitswap {
    class Bitswap : public libp2p::protocol::BaseProtocol,
        public std::enable_shared_from_this<Bitswap> {
    public:
        Bitswap(libp2p::Host& host,
                libp2p::event::Bus& event_bus);

        ~Bitswap() override = default;

        libp2p::peer::Protocol getProtocolId() const override;

        /**
         * In Bitswap, handle bitswap request
         */
        void handle(StreamResult stream_res) override;

        /**
         * Start accepting bitswap requests
         */
        void start();

        void Bitswap::RequestBlock(
            const libp2p::peer::PeerId& peer,
            boost::optional<libp2p::multi::Multiaddress> address,
            const libp2p::multi::ContentIdentifier& cid);
    private:
        /**
         * Handler for new connections, established by or with our host
         * @param conn - new connection
         */
        void onNewConnection(
            const std::weak_ptr<libp2p::connection::CapableConnection>& conn);
        void sendRequest(
            libp2p::protocol::BaseProtocol::StreamResult rstream,
            const libp2p::multi::ContentIdentifier& cid);

        void messageSent(
            libp2p::outcome::result<size_t> written_bytes, std::shared_ptr<libp2p::connection::Stream> stream);

        libp2p::Host& host_;
        libp2p::event::Bus& bus_;
        libp2p::event::Handle sub_;  // will unsubscribe during destruction by itself

        bool started_ = false;

        Logger logger_ = createLogger("Bitswap");
    };
}  // namespace libp2p::protocol

#endif  // IPFS_BITSWAP_HPP
