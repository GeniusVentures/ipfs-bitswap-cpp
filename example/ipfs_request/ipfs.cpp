#include <bitswap.hpp>
#include "logger.hpp"
#include <spdlog/sinks/basic_file_sink.h>

#include <boost/asio/io_context.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/log/configurator.hpp>
#include <libp2p/protocol/identify/identify.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <iostream>

namespace {
    const std::string logger_config(R"(
# ----------------
sinks:
  - name: console
    type: console
    color: false
groups:
  - name: main
    sink: console
    level: debug
    children:
      - name: libp2p
# ----------------
  )");
}  // namespace

int main(int argc, const char* argv[])
{
    auto logging_system = std::make_shared<soralog::LoggingSystem>(
        std::make_shared<soralog::ConfiguratorFromYAML>(
            // Original LibP2P logging config
            std::make_shared<libp2p::log::Configurator>(),
            // Additional logging config for application
            logger_config));
    auto r = logging_system->configure();
    libp2p::log::setLoggingSystem(logging_system);

    auto loggerProcessingEngine = sgns::ipfs_bitswap::createLogger("Bitswap");
    loggerProcessingEngine->set_level(spdlog::level::debug);

    auto injector = libp2p::injector::makeHostInjector();
    auto io = injector.create<std::shared_ptr<boost::asio::io_context>>();
    auto host = injector.create<std::shared_ptr<libp2p::Host>>();
    auto ma = libp2p::multi::Multiaddress::create("/ip4/127.0.0.1/tcp/40000").value();  // NOLINT

    auto self_id = host->getId();
    std::cerr << self_id.toBase58() << " * started" << std::endl;

    // Identify protocol initialization
    auto identityManager = injector.create<std::shared_ptr<libp2p::peer::IdentityManager>>();
    auto keyMarshaller = injector.create<std::shared_ptr<libp2p::crypto::marshaller::KeyMarshaller>>();

    auto identifyMessageProcessor = std::make_shared<libp2p::protocol::IdentifyMessageProcessor>(
        *host, host->getNetwork().getConnectionManager(), *identityManager, keyMarshaller);
    auto identify = std::make_shared<libp2p::protocol::Identify>(*host, identifyMessageProcessor, host->getBus());

    // CID source
    auto peer_address =
        libp2p::multi::Multiaddress::create(
            "/ip4/138.201.67.220/tcp/4001/p2p/QmNSYxZAiJHeLdkBg38roksAR9So7Y5eojks1yjEcUtZ7i"
            //"/ip4/10.0.65.121/tcp/4001/p2p/QmRXP6S7qwSH4vjSrZeJUGT68ww8rQVhoFWU5Kp7UkVkPN"
            //"/ip4/54.89.142.24/tcp/4001/p2p/QmRXP6S7qwSH4vjSrZeJUGT68ww8rQVhoFWU5Kp7UkVkPN"
        ).value();
    auto peer_id = libp2p::peer::PeerId::fromBase58(peer_address.getPeerId().value()).value();
    
    auto cid = libp2p::multi::ContentIdentifierCodec::fromString("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u").value();

    // Add peer to address repository
    libp2p::peer::PeerInfo pi{ peer_id, { peer_address } };
    auto upsert_res =
        host->getPeerRepository().getAddressRepository().upsertAddresses(
            pi.id,
            gsl::span(pi.addresses.data(), pi.addresses.size()),
            libp2p::peer::ttl::kPermanent);
    if (!upsert_res) 
    {
        std::cerr << pi.id.toBase58() << " was skipped at addind to peer routing table: "
            << upsert_res.error().message() << std::endl;
        return EXIT_FAILURE;
    }

    // Bitswap setup
    auto bitswap = std::make_shared<sgns::ipfs_bitswap::Bitswap>(*host);

    io->post([&] {
        auto listen = host->listen(ma);
        if (!listen)
        {
            std::cerr << "Cannot listen address " << ma.getStringAddress().data()
                << ". Error: " << listen.error().message() << std::endl;
            std::exit(EXIT_FAILURE);
        }

        identify->start();
        bitswap->start();
        host->start();

        bitswap->RequestBlock(peer_id, peer_address, cid);
        });

    boost::asio::signal_set signals(*io, SIGINT, SIGTERM);
    signals.async_wait(
        [&io](const boost::system::error_code&, int) { io->stop(); });
    io->run();

    return EXIT_SUCCESS;
}
