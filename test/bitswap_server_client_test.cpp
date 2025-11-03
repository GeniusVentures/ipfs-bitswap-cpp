#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <boost/format.hpp>
#include <boost/asio.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/injector/kademlia_injector.hpp>
#include <libp2p/injector/network_injector.hpp>
#include <libp2p/protocol/factory/protocol_factory.hpp>
#include <libp2p/security/noise.hpp>
#include <libp2p/log/logger.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include <libp2p/log/configurator.hpp>
#include <libp2p/peer/peer_repository.hpp>
#include <libp2p/peer/address_repository.hpp>
#include <libp2p/network/route_helper.hpp>
#include <boost/di/extension/scopes/shared.hpp>
#include <gsl/span>

#include "../src/bitswap.hpp"
#include "../src/logger.hpp"

using namespace sgns::ipfs_bitswap;

// Helper function to get local IP address that's not loopback
std::string getLocalIPAddress() {
    try {
        boost::asio::io_context io_context;
        boost::asio::ip::tcp::resolver resolver(io_context);
        boost::asio::ip::tcp::resolver::results_type endpoints = 
            resolver.resolve(boost::asio::ip::host_name(), "");
        
        std::string priority_ip;
        std::string fallback_ip;
        
        for (const auto& endpoint : endpoints) {
            auto addr = endpoint.endpoint().address();
            if (addr.is_v4() && !addr.is_loopback()) {
                std::string ip = addr.to_string();
                
                // Skip unspecified and link-local addresses
                if (ip == "0.0.0.0" || ip.find("169.254.") == 0) {
                    continue;
                }
                
                // Prioritize standard LAN addresses
                if (ip.find("192.168.") == 0 || ip.find("10.") == 0 || 
                    (ip.find("172.") == 0 && ip.substr(4, 1) >= "16" && ip.substr(4, 1) <= "31")) {
                    // Standard private network ranges (RFC 1918)
                    // 192.168.0.0/16, 10.0.0.0/8, 172.16.0.0/12
                    priority_ip = ip;
                    std::cout << "[DEBUG] Found priority LAN address: " << ip << std::endl;
                    break; // Use the first priority address we find
                } else if (fallback_ip.empty()) {
                    // Keep the first non-priority address as fallback (e.g., public IP, virtual interfaces)
                    fallback_ip = ip;
                    std::cout << "[DEBUG] Found fallback address: " << ip << std::endl;
                }
            }
        }
        
        if (!priority_ip.empty()) {
            return priority_ip;
        } else if (!fallback_ip.empty()) {
            std::cout << "[INFO] Using fallback address: " << fallback_ip << std::endl;
            return fallback_ip;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "[WARNING] Failed to get local IP address: " << e.what() << std::endl;
    }
    
    // Fallback to localhost if we can't find a better address
    std::cout << "[WARNING] No suitable IP address found, falling back to localhost" << std::endl;
    return "127.0.0.1";
}

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
        namespace di = boost::di;
        std::cout << "[" << node_name_ << "] Initializing bitswap node on port " << port_ << std::endl;
        
        // Create crypto providers with different instances per node to ensure unique keys
        auto csprng = std::make_shared<libp2p::crypto::random::BoostRandomGenerator>();
        auto ed25519_provider = std::make_shared<libp2p::crypto::ed25519::Ed25519ProviderImpl>();
        auto rsa_provider = std::make_shared<libp2p::crypto::rsa::RsaProviderImpl>();
        auto ecdsa_provider = std::make_shared<libp2p::crypto::ecdsa::EcdsaProviderImpl>();
        auto secp256k1_provider = std::make_shared<libp2p::crypto::secp256k1::Secp256k1ProviderImpl>();
        auto hmac_provider = std::make_shared<libp2p::crypto::hmac::HmacProviderImpl>();
        std::shared_ptr<libp2p::crypto::CryptoProvider> crypto_provider = std::make_shared<libp2p::crypto::CryptoProviderImpl>(
            csprng, ed25519_provider, rsa_provider, ecdsa_provider, secp256k1_provider, hmac_provider);
        auto validator = std::make_shared<libp2p::crypto::validator::KeyValidatorImpl>(crypto_provider);

        std::cout << "[" << node_name_ << "] Crypto providers created" << std::endl;
        
        // Use pre-generated different keys for each node to ensure uniqueness
        std::optional<libp2p::crypto::KeyPair> keys;
        
        keys = crypto_provider->generateKeys(libp2p::crypto::Key::Type::Ed25519).value();
        std::cout << "[" << node_name_ << "] Fixed keys created for port " << port_ << std::endl;
        
        // Create event bus
        event_bus_ = std::make_shared<libp2p::event::Bus>();
        
        std::cout << "[" << node_name_ << "] Creating host injector with fixed key binding..." << std::endl;
        
        // Create host with explicit key binding
        namespace di = boost::di;
        auto injector = libp2p::injector::makeHostInjector<di::extension::shared_config>(
            libp2p::injector::useSecurityAdaptors<libp2p::security::Noise>(),
            di::bind<libp2p::crypto::KeyPair>().to(std::move(*keys))[di::override],
            di::bind<libp2p::crypto::CryptoProvider>().to(crypto_provider)[di::override],
            di::bind<libp2p::crypto::random::CSPRNG>().to(std::move(csprng))[di::override],
            di::bind<libp2p::crypto::marshaller::KeyMarshaller>().TEMPLATE_TO<libp2p::crypto::marshaller::KeyMarshallerImpl>()[di::override],
            di::bind<libp2p::crypto::validator::KeyValidator>().TEMPLATE_TO(std::move(validator))[di::override]
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
        auto bindaddress = (boost::format("/ip4/%s/tcp/%d/p2p/%s") % "0.0.0.0" % port_ % host_->getId().toBase58()).str();
        
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
        // Get the actual local IP address (not localhost) for other nodes to connect to
        std::string local_ip = getLocalIPAddress();
        auto peer_addr_str = (boost::format("/ip4/%s/tcp/%d/p2p/%s") % local_ip % port_ % host_->getId().toBase58()).str();
        
        auto ma_res = libp2p::multi::Multiaddress::create(peer_addr_str);
        if (!ma_res) {
            std::cerr << "[ERROR] Failed to create peer multiaddress: " << ma_res.error().message() << std::endl;
            // Fallback to localhost
            peer_addr_str = (boost::format("/ip4/%s/tcp/%d/p2p/%s") % "127.0.0.1" % port_ % host_->getId().toBase58()).str();
            ma_res = libp2p::multi::Multiaddress::create(peer_addr_str);
        }
        
        auto ma = std::move(ma_res.value());
        auto peer_id_str = ma.getPeerId();
        auto peer_id_res = libp2p::peer::PeerId::fromBase58(*peer_id_str);
        
        return libp2p::peer::PeerInfo{peer_id_res.value(), {ma}};
    }

    std::shared_ptr<Bitswap> getBitswap() const {
        return bitswap_;
    }

    std::shared_ptr<libp2p::Host> getHost() const {
        return host_;
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
           // Give nodes time to initialize
        std::this_thread::sleep_for(std::chrono::seconds(2));     
        // Create client node on port 40301  
        client_node_ = std::make_unique<BitswapNode>("CLIENT", 40301);
        

        
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
        
        // Add server peer info to client's peer repository so it knows how to connect
        std::cout << "[SETUP] Adding server peer info to client..." << std::endl;
        auto client_host = client_node_->getHost();
        auto& peer_repo = client_host->getPeerRepository();
        auto& addr_repo = peer_repo.getAddressRepository();
        
        // Add server's addresses to client's peer repository
        auto add_result = addr_repo.upsertAddresses(
            server_peer_info.id,
            gsl::span(server_peer_info.addresses.data(), server_peer_info.addresses.size()),
            libp2p::peer::ttl::kDay
        );
        
        if (add_result) {
            std::cout << "[SETUP] Successfully added server peer info to client" << std::endl;
        } else {
            std::cerr << "[WARNING] Failed to add server peer info to client: " << add_result.error().message() << std::endl;
        }
        
        // Show server addresses for debugging
        std::cout << "[DEBUG] Server addresses:" << std::endl;
        for (const auto& addr : server_peer_info.addresses) {
            std::cout << "[DEBUG]   " << addr.getStringAddress() << std::endl;
        }
        
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
        
        // Wait for publish to complete - IO contexts are already running in their own threads
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(30);
        
        while (!publish_completed) {
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
        
        // Wait for request to complete - IO contexts are already running in their own threads
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(60);
        
        while (!request_completed) {
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