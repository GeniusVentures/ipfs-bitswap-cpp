#ifndef IPFS_BITSWAP_HPP
#define IPFS_BITSWAP_HPP

#include "logger.hpp"

#include <memory>
#include <vector>

//#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/base_protocol.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/multi/content_identifier.hpp>
#include <libp2p/connection/stream.hpp>
#include <libp2p/multi/uvarint.hpp>

//namespace libp2p::multi {
//  class Multiaddress;
//}

namespace sgns::ipfs_bitswap
{
class Session;
/**
* Implementation of an Bitswap protocol
*/
//class Bitswap : public libp2p::protocol::BaseProtocol, 
//    public std::enable_shared_from_this<Bitswap> 
class Bitswap: public std::enable_shared_from_this<Bitswap>
{
public:
    /**
        * Create an Identify instance; it will immediately start watching
        * connection events and react to them
        * @param msg_processor to work with Identify messages
        * @param event_bus - bus, over which the events arrive
        */
    Bitswap(libp2p::Host& host
        //std::shared_ptr<IdentifyMessageProcessor> msg_processor,
        //event::Bus& event_bus
    );

    //~Bitswap() override = default;

//    /**
//    * Get addresses other peers reported we have dialed from, when they
//    * provided a (\param address)
//    * @param address, for which to retrieve observed addresses
//    * @return set of addresses
//    */
//    //std::vector<multi::Multiaddress> getObservedAddressesFor(
//    //    const multi::Multiaddress& address) const;

    //libp2p::peer::Protocol getProtocolId() const override;

    /**
    * In Identify, handle means we are being identified by the other peer, so
    * we are expected to send the Identify message
    */
    //void handle(libp2p::protocol::BaseProtocol::StreamResult stream_res) override;

    /**
    * Start accepting NewConnectionEvent-s and asking each of them for Identify
    */
    void start();

    void RequestBlock(
        const libp2p::peer::PeerId& peer, 
        boost::optional<libp2p::multi::Multiaddress> address,
        const libp2p::multi::ContentIdentifier& cid);

private:
    void onStreamAccepted(libp2p::protocol::BaseProtocol::StreamResult rstream);
    void onStreamConnected(
        libp2p::protocol::BaseProtocol::StreamResult rstream,
        std::shared_ptr<std::vector<uint8_t>> request);
    void onNewStream(
        libp2p::protocol::BaseProtocol::StreamResult rstream, 
        std::shared_ptr<std::vector<uint8_t>> request);
    void onLengthRead(libp2p::outcome::result<libp2p::multi::UVarint> varint,
        std::shared_ptr<libp2p::connection::Stream> stream);
    void onMessageRead(libp2p::outcome::result<size_t> res,
        std::shared_ptr<std::vector<uint8_t>> buffer);
    bool createBlockRequest(
        const libp2p::multi::ContentIdentifier& cid,
        std::vector<uint8_t>& buffer);

//    /**
//        * Handler for new connections, established by or with our host
//        * @param conn - new connection
//        */
//    //void onNewConnection(
//    //    const std::weak_ptr<connection::CapableConnection>& conn);

    libp2p::Host& host_;
    //std::shared_ptr<IdentifyMessageProcessor> msg_processor_;
    //event::Bus& bus_;
    //event::Handle sub_;  // will unsubscribe during destruction by itself

    bool started_ = false;

    Logger logger_ = createLogger("Bitswap");

    std::shared_ptr<Session> session_;

};
} // sgns::ipfs_bitswap

#endif  // IPFS_BITSWAP_HPP

