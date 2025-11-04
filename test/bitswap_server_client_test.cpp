#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <cstring>
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
    std::unique_ptr<boost::asio::io_context::work> work_guard_;  // Keep IO context alive
    
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
        
        // Create work guard to keep IO context alive during complex operations
        work_guard_ = std::make_unique<boost::asio::io_context::work>(*io_context_);

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
        
        // Release work guard to allow IO context to exit
        work_guard_.reset();
        
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
    
    // Test data - complex directory structure
    struct TestFile {
        std::string path;
        std::string content;
    };
    
    std::vector<TestFile> test_files_ = {
        {"README.md", "# Project Documentation\n\nThis is the main README file for our test project.\nIt contains important information about the project structure and usage."},
        {"config.json", "{\n  \"name\": \"bitswap-test\",\n  \"version\": \"1.0.0\",\n  \"debug\": true,\n  \"ports\": [8080, 8081, 8082]\n}"},
        {"src/main.cpp", "#include <iostream>\n#include <string>\n\nint main() {\n    std::cout << \"Hello from bitswap test!\" << std::endl;\n    return 0;\n}"},
        {"src/utils.hpp", "#pragma once\n\n#include <string>\n#include <vector>\n\nnamespace utils {\n    std::string join(const std::vector<std::string>& parts, const std::string& delimiter);\n    std::vector<std::string> split(const std::string& input, char delimiter);\n}"},
        {"docs/api.md", "# API Documentation\n\n## Endpoints\n\n### GET /status\nReturns the current status of the service.\n\n### POST /data\nAccepts data for processing."},
        {"docs/examples/example1.txt", "This is example file 1.\nIt demonstrates basic functionality.\nLine 3 of example content."},
        {"docs/examples/example2.txt", "Example file 2 content here.\nThis shows how multiple files work together.\nFinal line of example 2."},
        {"tests/unit_test.cpp", "#include <gtest/gtest.h>\n\nTEST(BasicTest, SimpleAssertion) {\n    EXPECT_EQ(2 + 2, 4);\n    EXPECT_TRUE(true);\n}\n\nTEST(StringTest, Concatenation) {\n    std::string result = \"Hello\" + std::string(\" World\");\n    EXPECT_EQ(result, \"Hello World\");\n}"}
    };
    
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
        std::cout << "\\n[PUBLISH] Publishing complex directory structure to server..." << std::endl;
        std::cout << "[CONTENT] Creating directory structure with " << test_files_.size() << " files:" << std::endl;
        
        // Display the structure we're creating
        for (const auto& file : test_files_) {
            std::cout << "[FILE] " << file.path << " (" << file.content.size() << " bytes)" << std::endl;
        }
        
        // Create temporary directory structure for publishing
        std::string temp_dir = "temp_test_content";
        std::filesystem::path temp_path(temp_dir);
        
        try {
            // Clean up any existing temp directory
            if (std::filesystem::exists(temp_path)) {
                std::filesystem::remove_all(temp_path);
            }
            
            // Create the directory structure and files
            std::filesystem::create_directories(temp_path);
            
            for (const auto& file : test_files_) {
                std::filesystem::path file_path = temp_path / file.path;
                
                // Create parent directories if needed
                std::filesystem::create_directories(file_path.parent_path());
                
                // Write file content
                std::ofstream ofs(file_path, std::ios::binary);
                if (!ofs) {
                    std::cerr << "[ERROR] Failed to create file: " << file_path << std::endl;
                    return false;
                }
                ofs.write(file.content.data(), file.content.size());
                ofs.close();
                
                std::cout << "[CREATED] " << file_path << std::endl;
            }
            
            bool publish_completed = false;
            bool publish_success = false;
            std::string error_message;
            
            // Publish the entire directory structure
            server_node_->getBitswap()->PublishDirectory(
                temp_dir,
                [&](libp2p::outcome::result<CID> result) {
                    if (!result) {
                        error_message = result.error().message();
                        std::cerr << "[ERROR] Failed to publish directory: " << error_message << std::endl;
                        publish_success = false;
                    } else {
                        published_cid_ = result.value();
                        auto cid_str_result = libp2p::multi::ContentIdentifierCodec::toString(published_cid_.value());
                        if (cid_str_result) {
                            std::cout << "[SUCCESS] Directory structure published with CID: " << cid_str_result.value() << std::endl;
                        } else {
                            std::cout << "[SUCCESS] Directory structure published (CID encoding failed)" << std::endl;
                        }
                        publish_success = true;
                    }
                    publish_completed = true;
                }
            );
            
            // Wait for publish to complete - IO contexts are already running in their own threads
            auto start_time = std::chrono::steady_clock::now();
            const auto timeout = std::chrono::seconds(60); // Increased timeout for complex structure
            
            while (!publish_completed) {
                auto elapsed = std::chrono::steady_clock::now() - start_time;
                if (elapsed > timeout) {
                    std::cerr << "[TIMEOUT] Publish operation timed out" << std::endl;
                    return false;
                }
                
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            
            // Clean up temporary directory
            if (std::filesystem::exists(temp_path)) {
                std::filesystem::remove_all(temp_path);
                std::cout << "[CLEANUP] Temporary directory removed" << std::endl;
            }
            
            if (!publish_success) {
                std::cerr << "[FAILED] Publish failed: " << error_message << std::endl;
                return false;
            }
            
            std::cout << "[PUBLISH] Complex directory structure successfully published and available for retrieval" << std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "[ERROR] Exception during publish: " << e.what() << std::endl;
            
            // Clean up on error
            if (std::filesystem::exists(temp_path)) {
                try {
                    std::filesystem::remove_all(temp_path);
                } catch (...) {
                    // Ignore cleanup errors
                }
            }
            return false;
        }
    }

    bool testClientRetrieval() {
        std::cout << "\\n[RETRIEVE] Client requesting directory structure from server..." << std::endl;
        
        if (!published_cid_) {
            std::cerr << "[ERROR] No published CID available" << std::endl;
            return false;
        }
        
        auto cid_str_result = libp2p::multi::ContentIdentifierCodec::toString(published_cid_.value());
        if (cid_str_result) {
            std::cout << "[REQUEST] Directory CID: " << cid_str_result.value() << std::endl;
        } else {
            std::cout << "[REQUEST] Directory CID: (encoding failed)" << std::endl;
        }
        
        // Get server peer info
        auto server_peer_info = server_node_->getPeerInfo();
        std::cout << "[TARGET] Server peer: " << server_peer_info.id.toBase58() << std::endl;
        
        bool request_completed = false;
        bool request_success = false;
        std::string error_message;
        UnixFSContent retrieved_content;
        
        // Request directory structure from server
        client_node_->getBitswap()->RequestContent(
            server_peer_info, 
            published_cid_.value(), 
            [&](libp2p::outcome::result<UnixFSContent> result) {
                if (!result) {
                    error_message = result.error().message();
                    std::cerr << "[ERROR] Directory request failed: " << error_message << std::endl;
                    request_success = false;
                } else {
                    retrieved_content = std::move(result.value());
                    std::cout << "[SUCCESS] Directory structure retrieved successfully!" << std::endl;
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
                std::cerr << "[TIMEOUT] Directory request timed out" << std::endl;
                return false;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        if (!request_success) {
            std::cerr << "[FAILED] Directory request failed: " << error_message << std::endl;
            return false;
        }
        
        // Verify directory structure
        return verifyRetrievedContent(retrieved_content);
    }

    bool verifyRetrievedContent(const UnixFSContent& content) {
        std::cout << "\\n[VERIFY] Verifying retrieved directory structure..." << std::endl;
        
        // Check content type - should be a directory
        if (content.type != UnixFSContent::DIRECTORY) {
            std::cerr << "[ERROR] Expected directory, got type: " << static_cast<int>(content.type) << std::endl;
            return false;
        }
        std::cout << "[OK] Content type is directory" << std::endl;
        
        // Check number of files
        if (content.files.size() != test_files_.size()) {
            std::cerr << "[ERROR] File count mismatch. Expected: " << test_files_.size() 
                      << ", got: " << content.files.size() << std::endl;
            std::cerr << "[INFO] Retrieved files:" << std::endl;
            for (const auto& file : content.files) {
                std::cerr << "[INFO]   " << file.path << " (" << file.content.size() << " bytes)" << std::endl;
            }
            return false;
        }
        std::cout << "[OK] Directory contains " << content.files.size() << " files" << std::endl;
        
        // Create a map of retrieved files for easy lookup
        std::map<std::string, const UnixFSFile*> retrieved_files;
        for (const auto& file : content.files) {
            retrieved_files[file.path] = &file;
        }
        
        // Verify each expected file
        bool all_verified = true;
        for (const auto& expected_file : test_files_) {
            auto it = retrieved_files.find(expected_file.path);
            if (it == retrieved_files.end()) {
                std::cerr << "[ERROR] Missing file: " << expected_file.path << std::endl;
                all_verified = false;
                continue;
            }
            
            const auto* retrieved_file = it->second;
            
            // Check file size
            if (retrieved_file->content.size() != expected_file.content.size()) {
                std::cerr << "[ERROR] Size mismatch for " << expected_file.path 
                          << ". Expected: " << expected_file.content.size() 
                          << ", got: " << retrieved_file->content.size() << std::endl;
                all_verified = false;
                continue;
            }
            
            // Check content
            if (std::memcmp(retrieved_file->content.data(), expected_file.content.data(), expected_file.content.size()) != 0) {
                std::cerr << "[ERROR] Content mismatch for " << expected_file.path << std::endl;
                all_verified = false;
                continue;
            }
            
            std::cout << "[OK] ✅ " << expected_file.path << " (" << retrieved_file->content.size() << " bytes)" << std::endl;
        }
        
        if (all_verified) {
            std::cout << "\\n[SUCCESS] ✅ Directory structure verification passed!" << std::endl;
            std::cout << "[SUCCESS] All " << test_files_.size() << " files verified successfully:" << std::endl;
            for (const auto& expected_file : test_files_) {
                std::cout << "[SUCCESS]   " << expected_file.path << " (" << expected_file.content.size() << " bytes)" << std::endl;
            }
            return true;
        } else {
            std::cerr << "\\n[FAILED] ❌ Directory structure verification failed!" << std::endl;
            return false;
        }
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