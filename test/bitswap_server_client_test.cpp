#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <boost/asio/io_context.hpp>
#include <libp2p/host/basic_host.hpp>
#include <libp2p/security/noise.hpp>
#include <libp2p/transport/tcp.hpp>
#include <libp2p/muxer/yamux.hpp>
#include <libp2p/protocol/ping.hpp>
#include <libp2p/protocol/identify.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include <libp2p/multi/multiaddress.hpp>
#include <libp2p/peer/peer_info.hpp>
#include <libp2p/log/configurator.hpp>
#include <libp2p/log/logger.hpp>
#include <libp2p/crypto/key.hpp>
#include <libp2p/crypto/crypto_provider.hpp>
#include <libp2p/crypto/crypto_provider/crypto_provider_impl.hpp>
#include <libp2p/crypto/common.hpp>
#include <libp2p/crypto/random_generator/boost_generator.hpp>
#include <libp2p/crypto/ed25519_provider/ed25519_provider_impl.hpp>
#include <libp2p/crypto/rsa_provider/rsa_provider_impl.hpp>
#include <libp2p/crypto/ecdsa_provider/ecdsa_provider_impl.hpp>
#include <libp2p/crypto/secp256k1_provider/secp256k1_provider_impl.hpp>
#include <libp2p/crypto/hmac_provider/hmac_provider_impl.hpp>
#include <soralog/logging_system.hpp>
#include <soralog/impl/configurator_from_yaml.hpp>
#include <libp2p/protocol/factory/protocol_factory.hpp>
#include <boost/format.hpp>
#include <boost/di.hpp>

#include "../src/bitswap.hpp"
#include "../src/logger.hpp"

using namespace sgns::ipfs_bitswap;

class BitswapNode {
private:
    std::shared_ptr<boost::asio::io_context> io_context_;
    std::shared_ptr<libp2p::Host> host_;
    std::shared_ptr<Bitswap> bitswap_;
    std::shared_ptr<libp2p::event::Bus> event_bus_;
    std::thread io_thread_;
    std::string node_name_;
    int port_;
    bool is_running_;
    
public:
    BitswapNode(const std::string& name, int port) 
        : node_name_(name), port_(port), is_running_(false) {
        initializeNode();
    }

    ~BitswapNode() {
        shutdown();
    }

    void initializeNode() {
        std::cout << "[" << node_name_ << "] Initializing bitswap node on port " << port_ << std::endl;
        
        // Create crypto providers with different instances per node to ensure unique keys
        auto csprng = std::make_shared<libp2p::crypto::random::BoostRandomGenerator>();
        auto ed25519_provider = std::make_shared<libp2p::crypto::ed25519::Ed25519ProviderImpl>();
        auto rsa_provider = std::make_shared<libp2p::crypto::rsa::RsaProviderImpl>();
        auto ecdsa_provider = std::make_shared<libp2p::crypto::ecdsa::EcdsaProviderImpl>();
        auto secp256k1_provider = std::make_shared<libp2p::crypto::secp256k1::Secp256k1ProviderImpl>();
        auto hmac_provider = std::make_shared<libp2p::crypto::hmac::HmacProviderImpl>();
        auto crypto_provider = std::make_shared<libp2p::crypto::CryptoProviderImpl>(
            csprng, ed25519_provider, rsa_provider, ecdsa_provider, 
            secp256k1_provider, hmac_provider);
        
        std::cout << "[" << node_name_ << "] Crypto providers created" << std::endl;
        
        // Use pre-generated different keys for each node to ensure uniqueness
        libp2p::crypto::KeyPair keys;
        
        if (port_ == 40300) { // Server node
            // Server private key (32 bytes for Ed25519)
            std::vector<uint8_t> server_private_key = {
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 
                0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00
            };
            libp2p::crypto::PrivateKey private_key;
            private_key.type = libp2p::crypto::Key::Type::Ed25519;
            private_key.data = server_private_key;
            
            auto public_key_result = crypto_provider->derivePublicKey(private_key);
            if (!public_key_result) {
                throw std::runtime_error("Failed to derive server public key: " + public_key_result.error().message());
            }
            keys = libp2p::crypto::KeyPair{public_key_result.value(), private_key};
        } else { // Client node  
            // Client private key (different from server)
            std::vector<uint8_t> client_private_key = {
                0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88,
                0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
                0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89
            };
            libp2p::crypto::PrivateKey private_key;
            private_key.type = libp2p::crypto::Key::Type::Ed25519;
            private_key.data = client_private_key;
            
            auto public_key_result = crypto_provider->derivePublicKey(private_key);
            if (!public_key_result) {
                throw std::runtime_error("Failed to derive client public key: " + public_key_result.error().message());
            }
            keys = libp2p::crypto::KeyPair{public_key_result.value(), private_key};
        }
        
        std::cout << "[" << node_name_ << "] Fixed keys created for port " << port_ << std::endl;
        
        // Create event bus
        event_bus_ = std::make_shared<libp2p::event::Bus>();
        
        std::cout << "[" << node_name_ << "] Creating host injector with fixed key binding..." << std::endl;
        
        // Create host with explicit key binding
        namespace di = boost::di;
        auto injector = libp2p::injector::makeHostInjector(
            libp2p::injector::useSecurityAdaptors<libp2p::security::Noise>(),
            di::bind<libp2p::crypto::KeyPair>().to(std::move(keys))[di::override],
            di::bind<libp2p::crypto::CryptoProvider>().to(crypto_provider)[di::override],
            di::bind<libp2p::crypto::random::CSPRNG>().to(std::move(csprng))[di::override]
        );
        
        std::cout << "[" << node_name_ << "] Injector created, creating host..." << std::endl;
        
        host_ = injector.create<std::shared_ptr<libp2p::Host>>();
        
        std::cout << "[" << node_name_ << "] Host created" << std::endl;
        
        // Get the IO context from the same injector
        io_context_ = injector.create<std::shared_ptr<boost::asio::io_context>>();

        // Config Protocols
        libp2p::protocol::factory::ProtocolFactory::ProtocolConfig protocol_config;
        protocol_config.enable_identify = true;
        protocol_config.enable_autonat = false;
        protocol_config.enable_relay = false;
        protocol_config.enable_holepunch_server = false;
        protocol_config.enable_holepunch_client = false;

        auto protocols = libp2p::protocol::factory::ProtocolFactory::createProtocols(host_, protocol_config, injector);
        protocols.identify->start();
        
        // Bind Listen Address
        auto bindaddress = (boost::format("/ip4/%s/tcp/%d/p2p/%s") % "127.0.0.1" % port_ % host_->getId().toBase58()).str();
        
        std::vector<libp2p::multi::Multiaddress> multiaddresses;
        auto ma_res = libp2p::multi::Multiaddress::create(bindaddress);
        
        auto ma = std::move(ma_res.value());
        boost::optional<libp2p::peer::PeerId> peer_id;
        auto peer_id_str = ma.getPeerId();
        
        multiaddresses.push_back(ma);
        auto peer_id_res = libp2p::peer::PeerId::fromBase58(*peer_id_str);
        peer_id = peer_id_res.value();
        
        auto peerInfo = libp2p::peer::PeerInfo{*peer_id, multiaddresses};
        
        host_->listen(peerInfo.addresses[0]);
        host_->start();
        
        // Create bitswap instance and initialize it
        bitswap_ = std::make_shared<Bitswap>(*host_, *event_bus_, io_context_);
        bitswap_->initialize();
        
        // Start IO thread
        io_thread_ = std::thread([this]() { 
            io_context_->run(); 
        });
        
        is_running_ = true;
        
        std::cout << "[" << node_name_ << "] Node initialized with ID: " << host_->getId().toBase58() << std::endl;
        std::cout << "[" << node_name_ << "] Listening on: " << bindaddress << std::endl;
    }

    void shutdown() {
        if (!is_running_) return;
        
        std::cout << "[" << node_name_ << "] Shutting down..." << std::endl;
        
        // Stop the host first
        if (host_) {
            host_->stop();
        }
        
        // Stop the IO context
        if (io_context_ && !io_context_->stopped()) {
            io_context_->stop();
        }
        
        // Wait for the thread to finish
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
        
        is_running_ = false;
        std::cout << "[" << node_name_ << "] Shutdown complete" << std::endl;
    }

    libp2p::peer::PeerInfo getPeerInfo() const {
        auto bindaddress = (boost::format("/ip4/%s/tcp/%d/p2p/%s") % "127.0.0.1" % port_ % host_->getId().toBase58()).str();
        auto ma_res = libp2p::multi::Multiaddress::create(bindaddress);
        auto ma = std::move(ma_res.value());
        auto peer_id_str = ma.getPeerId();
        auto peer_id_res = libp2p::peer::PeerId::fromBase58(*peer_id_str);
        
        return libp2p::peer::PeerInfo{peer_id_res.value(), {ma}};
    }

    std::shared_ptr<Bitswap> getBitswap() const {
        return bitswap_;
    }

    std::shared_ptr<boost::asio::io_context> getIOContext() const {
        return io_context_;
    }

    const std::string& getName() const {
        return node_name_;
    }
};

class BitswapServerClientTest {
private:
    std::unique_ptr<BitswapNode> server_node_;
    std::unique_ptr<BitswapNode> client_node_;
    
    // Test data
    const std::string test_content_ = "Hello, this is a test message from the bitswap server!\\nThis demonstrates bidirectional communication.\\nLine 3 of test data.";
    std::optional<CID> published_cid_;
    
public:
    BitswapServerClientTest() {
        initializeLogging();
        std::cout << "\\n[TEST] Starting Bitswap Server-Client Test" << std::endl;
        std::cout << "==========================================" << std::endl;
    }

    void initializeLogging() {
        const std::string logger_config(R"(
# ----------------
sinks:
  - name: console
    type: console
    color: false
groups:
  - name: main
    sink: console
    level: info
    children:
      - name: libp2p
        level: warn
      - name: bitswap
        level: debug
# ----------------
        )");

        auto logging_system = std::make_shared<soralog::LoggingSystem>(
            std::make_shared<soralog::ConfiguratorFromYAML>(
                std::make_shared<libp2p::log::Configurator>(),
                logger_config));
        auto r = logging_system->configure();
        libp2p::log::setLoggingSystem(logging_system);
        
        auto loggerBitswap = sgns::ipfs_bitswap::createLogger("Bitswap");
        loggerBitswap->set_level(spdlog::level::debug);
    }

    bool setupNodes() {
        std::cout << "\\n[SETUP] Creating server and client nodes..." << std::endl;
        
        // Create server node on port 40300
        server_node_ = std::make_unique<BitswapNode>("SERVER", 40300);
        
        // Create client node on port 40301  
        client_node_ = std::make_unique<BitswapNode>("CLIENT", 40301);
        
        // Give nodes time to initialize
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // Check if nodes have different peer IDs
        auto server_peer_info = server_node_->getPeerInfo();
        auto client_peer_info = client_node_->getPeerInfo();
        
        if (server_peer_info.id == client_peer_info.id) {
            std::cerr << "[ERROR] Both nodes have the same peer ID: " << server_peer_info.id.toBase58() << std::endl;
            std::cerr << "[ERROR] This will prevent connection. The libp2p key generation might be deterministic." << std::endl;
            return false;
        }
        
        std::cout << "[SETUP] Server peer ID: " << server_peer_info.id.toBase58() << std::endl;
        std::cout << "[SETUP] Client peer ID: " << client_peer_info.id.toBase58() << std::endl;
        std::cout << "[SETUP] Both nodes created successfully with different peer IDs" << std::endl;
        return true;
    }

    bool publishTestContent() {
        std::cout << "\\n[PUBLISH] Publishing test content to server..." << std::endl;
        std::cout << "[CONTENT] Data: '" << test_content_ << "'" << std::endl;
        std::cout << "[CONTENT] Size: " << test_content_.size() << " bytes" << std::endl;
        
        bool publish_completed = false;
        bool publish_success = false;
        std::string error_message;
        
        // Publish the test content as raw data
        server_node_->getBitswap()->PublishData(
            std::vector<uint8_t>(test_content_.begin(), test_content_.end()),
            [&](libp2p::outcome::result<CID> result) {
                if (!result) {
                    error_message = result.error().message();
                    std::cerr << "[ERROR] Failed to publish content: " << error_message << std::endl;
                    publish_success = false;
                } else {
                    published_cid_ = result.value();
                    auto cid_str_result = libp2p::multi::ContentIdentifierCodec::toString(published_cid_.value());
                    if (cid_str_result) {
                        std::cout << "[SUCCESS] Content published with CID: " << cid_str_result.value() << std::endl;
                    } else {
                        std::cout << "[SUCCESS] Content published (CID encoding failed)" << std::endl;
                    }
                    publish_success = true;
                }
                publish_completed = true;
            }
        );
        
        // Wait for publish to complete
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(30);
        
        while (!publish_completed) {
            server_node_->getIOContext()->poll_one();
            client_node_->getIOContext()->poll_one();
            
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed > timeout) {
                std::cerr << "[TIMEOUT] Publish operation timed out" << std::endl;
                return false;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        if (!publish_success) {
            std::cerr << "[FAILED] Publish failed: " << error_message << std::endl;
            return false;
        }
        
        std::cout << "[PUBLISH] Content successfully published and available for retrieval" << std::endl;
        return true;
    }

    bool testClientRetrieval() {
        std::cout << "\\n[RETRIEVE] Client requesting content from server..." << std::endl;
        
        if (!published_cid_) {
            std::cerr << "[ERROR] No published CID available" << std::endl;
            return false;
        }
        
        auto cid_str_result = libp2p::multi::ContentIdentifierCodec::toString(published_cid_.value());
        if (cid_str_result) {
            std::cout << "[REQUEST] CID: " << cid_str_result.value() << std::endl;
        } else {
            std::cout << "[REQUEST] CID: (encoding failed)" << std::endl;
        }
        
        // Get server peer info
        auto server_peer_info = server_node_->getPeerInfo();
        std::cout << "[TARGET] Server peer: " << server_peer_info.id.toBase58() << std::endl;
        
        bool request_completed = false;
        bool request_success = false;
        std::string error_message;
        UnixFSContent retrieved_content;
        
        // Request content from server
        client_node_->getBitswap()->RequestContent(
            server_peer_info, 
            published_cid_.value(), 
            [&](libp2p::outcome::result<UnixFSContent> result) {
                if (!result) {
                    error_message = result.error().message();
                    std::cerr << "[ERROR] Content request failed: " << error_message << std::endl;
                    request_success = false;
                } else {
                    retrieved_content = std::move(result.value());
                    std::cout << "[SUCCESS] Content retrieved successfully!" << std::endl;
                    request_success = true;
                }
                request_completed = true;
            }
        );
        
        // Wait for request to complete
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(60);
        
        while (!request_completed) {
            server_node_->getIOContext()->poll_one();
            client_node_->getIOContext()->poll_one();
            
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed > timeout) {
                std::cerr << "[TIMEOUT] Content request timed out" << std::endl;
                return false;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        if (!request_success) {
            std::cerr << "[FAILED] Request failed: " << error_message << std::endl;
            return false;
        }
        
        // Verify content
        return verifyRetrievedContent(retrieved_content);
    }

    bool verifyRetrievedContent(const UnixFSContent& content) {
        std::cout << "\\n[VERIFY] Verifying retrieved content..." << std::endl;
        
        // Check content type
        if (content.type != UnixFSContent::SINGLE_FILE) {
            std::cerr << "[ERROR] Expected single file, got type: " << static_cast<int>(content.type) << std::endl;
            return false;
        }
        std::cout << "[OK] Content type is single file" << std::endl;
        
        // Check number of files
        if (content.files.size() != 1) {
            std::cerr << "[ERROR] Expected 1 file, got: " << content.files.size() << std::endl;
            return false;
        }
        std::cout << "[OK] Content contains exactly 1 file" << std::endl;
        
        const auto& file = content.files[0];
        
        // Check file size
        if (file.content.size() != test_content_.size()) {
            std::cerr << "[ERROR] Size mismatch. Expected: " << test_content_.size() 
                      << ", got: " << file.content.size() << std::endl;
            return false;
        }
        std::cout << "[OK] File size matches: " << file.content.size() << " bytes" << std::endl;
        
        // Check content
        std::string retrieved_text(file.content.begin(), file.content.end());
        if (retrieved_text != test_content_) {
            std::cerr << "[ERROR] Content mismatch!" << std::endl;
            std::cerr << "Expected: '" << test_content_ << "'" << std::endl;
            std::cerr << "Got:      '" << retrieved_text << "'" << std::endl;
            return false;
        }
        std::cout << "[OK] Content matches exactly!" << std::endl;
        
        std::cout << "[CONTENT] Retrieved: '" << retrieved_text << "'" << std::endl;
        
        std::cout << "\\n[SUCCESS] Content verification passed!" << std::endl;
        return true;
    }

    void run() {
        bool success = true;
        
        try {
            // Setup nodes
            if (!setupNodes()) {
                std::cerr << "[FAILED] Node setup failed" << std::endl;
                return;
            }
            
            // Publish content on server
            if (!publishTestContent()) {
                std::cerr << "[FAILED] Content publishing failed" << std::endl;
                success = false;
            }
            
            // Test client retrieval
            if (success && !testClientRetrieval()) {
                std::cerr << "[FAILED] Content retrieval failed" << std::endl;
                success = false;
            }
            
            if (success) {
                std::cout << "\\n[COMPLETE] ✅ All tests passed! Bitswap server-client communication working!" << std::endl;
            } else {
                std::cout << "\\n[COMPLETE] ❌ Some tests failed!" << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cerr << "[EXCEPTION] Test failed with exception: " << e.what() << std::endl;
        }
    }
};

int main() {
    try {
        BitswapServerClientTest test;
        test.run();
        
        // Give some time for cleanup
        std::cout << "\\n[CLEANUP] Waiting for cleanup..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[FAILED] Test failed with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "[FAILED] Test failed with unknown exception" << std::endl;
        return 1;
    }
}