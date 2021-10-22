#include "bitswap.hpp"

#include <string>
#include <tuple>
#include <memory>

#include <boost/assert.hpp>
//#include <boost/di.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <proto/bitswap.pb.h>
#include <libp2p/basic/varint_reader.hpp>
#include <gsl/span>

namespace
{
    const std::string bitswapProtocolId = "/ipfs/bitswap/1.0.0";
}  // namespace

namespace sgns::ipfs_bitswap
{
class Session : public std::enable_shared_from_this<Session>
{
public:
    explicit Session(std::shared_ptr<libp2p::connection::Stream> stream, Logger logger)
        : stream_(std::move(stream))
        , logger_(logger)
        , incoming_(std::make_shared<std::vector<uint8_t>>(1 << 12)) 
    {
    };

    bool read()
    {
        if (stream_->isClosedForRead())
        {
            close();
            return false;
        }

        stream_->readSome(
            gsl::span(incoming_->data(), static_cast<ssize_t>(incoming_->size())),
            incoming_->size(),
            [self = shared_from_this()](libp2p::outcome::result<size_t> result)
        {
            if (!result)
            {
                self->close();
                self->logger_->debug("{} - closed at reading", self->stream_->remotePeerId().value().toBase58());
                return;
            }
            self->logger_->debug("data read from stream {} > {}", self->stream_->remotePeerId().value().toBase58(),
                std::string(self->incoming_->begin(), self->incoming_->begin() + static_cast<ssize_t>(result.value())));
            self->read();
        });
        return true;
    }

    bool write(const std::shared_ptr<std::vector<uint8_t>>& buffer)
    {
        if (stream_->isClosedForWrite()) {
            close();
            return false;
        }

        stream_->write(
            gsl::span(buffer->data(), static_cast<ssize_t>(buffer->size())),
            buffer->size(),
            [self = shared_from_this(),
            buffer](libp2p::outcome::result<size_t> result) {
                if (!result) {
                    self->close();
                    self->logger_->debug("{} - closed at writting", self->stream_->remotePeerId().value().toBase58());
                    return;
                }
                self->logger_->debug("data written to stream {} < {} from {} bytes", 
                    self->stream_->remotePeerId().value().toBase58(), 
                    //std::string(buffer->begin(), buffer->begin() + static_cast<ssize_t>(result.value()))
                    static_cast<ssize_t>(result.value()), buffer->size()
                );
            }
        );
        return true;
    }

    void close()
    {
        stream_->close([self = shared_from_this()](auto) {});
        //sessions.erase(shared_from_this());
    }

    bool operator<(const Session& other)
    {
        return stream_->remotePeerId().value()
            < other.stream_->remotePeerId().value();
    }

private:
    std::shared_ptr<libp2p::connection::Stream> stream_;
    std::shared_ptr<std::vector<uint8_t>> incoming_;
    Logger logger_;
};

Bitswap::Bitswap(libp2p::Host& host)
: host_{ host }
{
}

//libp2p::peer::Protocol Bitswap::getProtocolId() const
//{
//    return bitswapProtocolId;
//}

void Bitswap::start() 
{
    // no double starts
    BOOST_ASSERT(!started_);
    started_ = true;
    logger_->debug("bitswap started");

    host_.setProtocolHandler(
        bitswapProtocolId,
        [wp = weak_from_this()](libp2p::protocol::BaseProtocol::StreamResult rstream) {
        if (auto self = wp.lock()) {
            self->onStreamAccepted(std::move(rstream));
        }
});

//sub_ = bus_.getChannel<network::event::OnNewConnectionChannel>().subscribe(
//    [wp = weak_from_this()](auto&& conn) {
//    if (auto self = wp.lock()) {
//        return self->onNewConnection(conn);
//    }
//});
}

//void Bitswap::onNewConnection(
//    const std::weak_ptr<connection::CapableConnection>& conn) {
//    if (conn.expired()) {
//        return;
//    }
//
//    auto remote_peer_res = conn.lock()->remotePeer();
//    if (!remote_peer_res) {
//        return;
//    }
//
//    auto remote_peer_addr_res = conn.lock()->remoteMultiaddr();
//    if (!remote_peer_addr_res) {
//        return;
//    }
//
//    peer::PeerInfo peer_info{ std::move(remote_peer_res.value()),
//                                std::vector<multi::Multiaddress>{
//                                    std::move(remote_peer_addr_res.value())} };
//
//    msg_processor_->getHost().newStream(
//        peer_info, kIdentifyProto,
//        [self{ shared_from_this() }](auto&& stream_res) {
//        if (!stream_res) {
//            return;
//        }
//        self->msg_processor_->receiveIdentify(std::move(stream_res.value()));
//    });
//}

bool createBlockRequest(
    const libp2p::multi::ContentIdentifier& cid,
    std::vector<uint8_t>& buffer)
{
    bitswap_pb::Message pb_msg;
    auto wantlist = pb_msg.mutable_wantlist();
    auto entry = wantlist->add_entries();
    entry->set_block(libp2p::multi::ContentIdentifierCodec::toString(cid).value());
    entry->set_priority(1);
    entry->set_cancel(false);
    entry->set_wanttype(bitswap_pb::Message_Wantlist_WantType_Block);
    entry->set_senddonthave(false);

    size_t msg_sz = pb_msg.ByteSizeLong();
    auto varint_len = libp2p::multi::UVarint{ msg_sz };
    auto varint_vec = varint_len.toVector();
    size_t prefix_sz = varint_vec.size();
    buffer.resize(prefix_sz + msg_sz);
    memcpy(buffer.data(), varint_vec.data(), prefix_sz);
    return pb_msg.SerializeToArray(buffer.data() + prefix_sz, msg_sz);

}

void Bitswap::RequestBlock(
    const libp2p::peer::PeerId& peer,
    boost::optional<libp2p::multi::Multiaddress> address,
    const libp2p::multi::ContentIdentifier& cid)
{
    // Create biswap request
    auto serialized_request = std::make_shared<std::vector<uint8_t>>();
    if (!createBlockRequest(cid, *serialized_request))
    {
        logger_->error("Serialize error");
        return;
    }

    libp2p::peer::PeerInfo pi{ peer, {address.value()} };

    // Check if connectable
    auto connectedness = host_.connectedness(pi);
    if (connectedness == libp2p::Host::Connectedness::CAN_NOT_CONNECT)
    {
        logger_->debug("Peer {} is not connectible", pi.id.toBase58());
    }

    host_.newStream(
        pi,
        bitswapProtocolId,
        [wp = weak_from_this(), serialized_request](libp2p::protocol::BaseProtocol::StreamResult rstream) 
    {
        auto ctx = wp.lock();
        if (ctx)
        {
            ctx->onStreamConnected(std::move(rstream), serialized_request);
        }
    });

}

void Bitswap::onStreamAccepted(libp2p::protocol::BaseProtocol::StreamResult rstream)
{
    if (!started_)
    {
        return;
    }

    if (!rstream)
    {
        logger_->error("accept error, msg='{}'", rstream.error().message());
        return;
    }

    auto stream = rstream.value();

    auto peer_id_res = stream->remotePeerId();
    if (!peer_id_res)
    {
        logger_->error("no peer id for accepted stream, msg='{}'",
            rstream.error().message());
        return;
    }

    //auto ctx = findContext(peer_id_res.value(), true);
    logger_->trace("accepted stream from peer={}", stream->remotePeerId().value().toBase58());
    //ctx->onStreamAccepted(std::move(rstream.value()));

    // TODO: Move to session class. An example can be found in libp2p kademlia session.
    if (stream->isClosedForRead()) {
        //close(Error::STREAM_RESET);
        logger_->error("stream is closed");
        return;
    }

    //if (closed_) {
    //    return;
    //}

    libp2p::basic::VarintReader::readVarint(
        stream,
        [wp = weak_from_this(), stream](libp2p::outcome::result<libp2p::multi::UVarint> varint) {
        if (auto self = wp.lock())
            self->onLengthRead(std::move(varint), std::move(stream));
    });
}

void Bitswap::onLengthRead(libp2p::outcome::result<libp2p::multi::UVarint> varint,
    std::shared_ptr<libp2p::connection::Stream> stream)
{
    if (stream->isClosedForRead()) {
        //close(Error::STREAM_RESET);
        logger_->error("stream is closed");
        return;
    }

    //if (closed_) {
    //    return;
    //}

    if (varint.has_error()) {
        //close(varint.error());
        logger_->error("varint error {}", varint.error());
        return;
    }

    auto msg_len = varint.value().toUInt64();
    auto buffer = std::make_shared<std::vector<uint8_t>>();
    buffer->resize(msg_len);

    stream->read(gsl::span(buffer->data(), buffer->size()),
        msg_len, 
        [wp = weak_from_this(), buffer](libp2p::outcome::result<size_t> res) {
            if (auto self = wp.lock()) 
            {
                self->onMessageRead(std::forward<decltype(res)>(res), std::move(buffer));
            }
        });
}

void Bitswap::onMessageRead(libp2p::outcome::result<size_t> res,
    std::shared_ptr<std::vector<uint8_t>> buffer)
{
    //cancelReadingTimeout();

    //if (closed_) 
    //{
    //    return;
    //}

    if (!res) 
    {
        //close(res.as_failure());
        logger_->error("failed result received");
        return;
    }

    if (buffer->size() != res.value()) {
        //close(Error::MESSAGE_PARSE_ERROR);
        logger_->error("corrupted stream size");
        return;
    }

    logger_->debug("{} bytes read", res.value());

    bitswap_pb::Message msg;
    if (!msg.ParseFromArray(buffer->data(), buffer->size())) 
    {
        //close(Error::MESSAGE_DESERIALIZE_ERROR);
        logger_->error("MESSAGE_DESERIALIZE_ERROR");
        return;
    }

    logger_->debug("{} blocks received", msg.blocks_size());
    for (int i = 0; i < msg.blocks_size(); ++i)
    {
        logger_->debug("block[{}]: {}", i, msg.blocks()[i]);
    }

    // Propogate to session host
    //if (!pocessed) 
    //{
    //    if (auto session_host = session_host_.lock()) {
    //        session_host->onMessage(shared_from_this(), std::move(msg));
    //    }
    //}

    //// Continue to wait some response
    //if (!response_handlers_.empty()) 
    //{
    //    read();
    //}

    //if (canBeClosed()) 
    //{
    //    close();
    //}
}

void Bitswap::onStreamConnected(
    libp2p::protocol::BaseProtocol::StreamResult rstream,
    std::shared_ptr<std::vector<uint8_t>> request)
{
    //if (closed_) {
    //    return;
    //}
    if (rstream)
    {
        //logger_->debug("connected to peer={}", str);
        onNewStream(std::move(rstream.value()), request);
    }
    else 
    {
        logger_->error(
          "cannot connect to remote peer, msg='{}'", rstream.error().message());
        //if (getState() == is_connecting) {
        //    closeLocalRequests(RS_CANNOT_CONNECT);
        //}
    }
}

void Bitswap::onNewStream(
    libp2p::protocol::BaseProtocol::StreamResult rstream,
    std::shared_ptr<std::vector<uint8_t>> request)
{
    auto stream = rstream.value();

    std::string addr(stream->remoteMultiaddr().value().getStringAddress());
    logger_->debug("connected to {}", addr);
    logger_->debug("outgoing stream with {}", stream->remotePeerId().value().toBase58());
    auto session = std::make_shared<Session>(stream, logger_);
    if (session->write(request))
    {
        logger_->debug("request sent to {}", addr);
    }
    else
    {
        logger_->error("request cannot be sent sent to {}", addr);
    }
}
} // sgns::ipfs_bitswap

