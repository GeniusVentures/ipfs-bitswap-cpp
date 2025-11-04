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
#include <soralog/logging_system.hpp>
#include <soralog/impl/configurator_from_yaml.hpp>
#include <libp2p/protocol/factory/protocol_factory.hpp>
#include <boost/format.hpp>

#include "../src/bitswap.hpp"
#include "../src/logger.hpp"

using namespace sgns::ipfs_bitswap;

class BitswapContentTest {
private:
    std::shared_ptr<boost::asio::io_context> io_context_;
    std::shared_ptr<libp2p::Host> host_;
    std::shared_ptr<Bitswap> bitswap_;
    std::shared_ptr<libp2p::event::Bus> event_bus_;
    std::thread m_thread;
    std::unique_ptr<boost::asio::io_context::work> work_guard_;  // Keep IO context alive
    
public:
    BitswapContentTest() {
        // Initialize logging system
        const std::string logger_config(R"(
# ----------------
sinks:
  - name: console
    type: console
    color: false
groups:
  - name: main
    sink: console
    level: error
    children:
      - name: libp2p
      - name: bitswap
# ----------------
        )");

        auto logging_system = std::make_shared<soralog::LoggingSystem>(
            std::make_shared<soralog::ConfiguratorFromYAML>(
                // Original LibP2P logging config
                std::make_shared<libp2p::log::Configurator>(),
                // Additional logging config for application
                logger_config));
        auto r = logging_system->configure();
        libp2p::log::setLoggingSystem(logging_system);
        
        auto loggerBitswap         = sgns::ipfs_bitswap::createLogger( "Bitswap" );
        loggerBitswap->set_level( spdlog::level::trace );
        // Create event bus
        event_bus_ = std::make_shared<libp2p::event::Bus>();
        
        // Create host with libp2p injector - configure to only use Noise security (no plaintext)
        auto injector = libp2p::injector::makeHostInjector(
            libp2p::injector::useSecurityAdaptors<libp2p::security::Noise>()
        );
        host_ = injector.create<std::shared_ptr<libp2p::Host>>();
        
        // Get the IO context from the same injector (crucial - must be same as host uses!)
        io_context_ = injector.create<std::shared_ptr<boost::asio::io_context>>();
        
        // Create work guard to keep IO context alive during complex operations
        work_guard_ = std::make_unique<boost::asio::io_context::work>(*io_context_);

        //Config Protocols
        libp2p::protocol::factory::ProtocolFactory::ProtocolConfig protocol_config;
        protocol_config.enable_identify = true;
        protocol_config.enable_autonat = false;
        protocol_config.enable_relay = false;
        protocol_config.enable_holepunch_server = false;
        protocol_config.enable_holepunch_client = false;

        auto protocols = libp2p::protocol::factory::ProtocolFactory::createProtocols(host_, protocol_config, injector);
        protocols.identify->start();
        //Bind Listen Address
        auto bindaddress = (boost::format("/ip4/%s/tcp/%d/p2p/%s") % "0.0.0.0" % "40200" % host_->getId().toBase58()).str();

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
        //Start Host
        host_->start();
        // Create bitswap instance and initialize it (registers protocol handler)
        bitswap_ = std::make_shared<Bitswap>(*host_, *event_bus_, io_context_);
        bitswap_->initialize();  // Call after shared_ptr is constructed to avoid bad_weak_ptr
        
        m_thread = std::thread([this]() { io_context_->run(); });
        std::cout << "Libp2p ID" << host_->getId().toBase58() << std::endl;
        std::cout << "[OK] Bitswap test environment initialized" << std::endl;
    }

    ~BitswapContentTest() {
        shutdown();
    }

    void shutdown() {
        std::cout << "\n[SHUTDOWN] Stopping test environment..." << std::endl;
        
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
        if (m_thread.joinable()) {
            m_thread.join();
            std::cout << "[OK] IO thread terminated" << std::endl;
        }
        
        std::cout << "[OK] Shutdown complete" << std::endl;
    }



    bool connectToIPFSNode(const std::string& multiaddr_str) {
        std::cout << "\n[CONNECT] Connecting to IPFS node: " << multiaddr_str << std::endl;
        
        // Parse the multiaddress
        auto multiaddr_result = libp2p::multi::Multiaddress::create(multiaddr_str);
        if (!multiaddr_result) {
            std::cerr << "[ERROR] Failed to parse multiaddress: " << multiaddr_result.error().message() << std::endl;
            return false;
        }
        
        auto multiaddr = multiaddr_result.value();
        std::cout << "[OK] Parsed multiaddress successfully: " << multiaddr.getStringAddress() << std::endl;
        
        // Extract peer info
        auto peer_id_result = multiaddr.getPeerId();
        if (!peer_id_result) {
            std::cerr << "[ERROR] Failed to extract peer ID from multiaddress" << std::endl;
            return false;
        }
        
        auto peer_id = libp2p::peer::PeerId::fromBase58(peer_id_result.value());
        if (!peer_id) {
            std::cerr << "[ERROR] Failed to parse peer ID: " << peer_id.error().message() << std::endl;
            return false;
        }
        
        peer_info_ = libp2p::peer::PeerInfo{peer_id.value(), {multiaddr}};
        std::cout << "[OK] Extracted peer info - ID: " << peer_info_.value().id.toBase58() << std::endl;
        return true;
    }
    
    void testContentRetrieval(const std::string& cid_str) {
        std::cout << "\n[RETRIEVE] Testing content retrieval for CID: " << cid_str << std::endl;
        
        // Parse the CID
        auto cid_result = libp2p::multi::ContentIdentifierCodec::fromString(cid_str);
        if (!cid_result) {
            std::cerr << "[ERROR] Failed to parse CID: " << cid_result.error().message() << std::endl;
            return;
        }
        
        auto cid = cid_result.value();
        std::cout << "[OK] Parsed CID successfully" << std::endl;
        
        // Set up completion tracking
        bool request_completed = false;
        std::string error_message;
        UnixFSContent retrieved_content;
        
        // Request content using our enhanced API
        std::cout << "[START] Starting content request..." << std::endl;
        
        bitswap_->RequestContent(peer_info_.value(), cid, [&](libp2p::outcome::result<UnixFSContent> result) {
            if (!result) {
                error_message = result.error().message();
                std::cerr << "[ERROR] Content request failed: " << error_message << std::endl;
            } else {
                retrieved_content = std::move(result.value());
                std::cout << "[SUCCESS] Content request completed successfully!" << std::endl;
            }
            request_completed = true;
        });
        
        // Run the event loop with timeout
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(130);
        
        while (!request_completed) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed > timeout) {
                std::cerr << "[TIMEOUT] Request timed out after 30 seconds" << std::endl;
                return;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        // Display results
        if (!error_message.empty()) {
            std::cerr << "\n[FAILED] Request failed with error: " << error_message << std::endl;
            return;
        }
        
        // Wait a bit longer to ensure all background processing completes
        std::cout << "\n[WAIT] Ensuring all content processing is complete..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        displayContentResults(retrieved_content);
    }
    
    void displayContentResults(const UnixFSContent& content) {
        std::cout << "\n[RESULTS] Content Analysis Results:" << std::endl;
        std::cout << "==============================" << std::endl;
        
        // Display content type
        std::string type_str;
        switch (content.type) {
            case UnixFSContent::SINGLE_FILE:
                type_str = "Single File";
                break;
            case UnixFSContent::DIRECTORY:
                type_str = "Directory";
                break;
            case UnixFSContent::MULTI_FILE_ARCHIVE:
                type_str = "Multi-File Archive";
                break;
        }
        std::cout << "[FILE] Content Type: " << type_str << std::endl;
        std::cout << "[COUNT] Total Files: " << content.files.size() << std::endl;
        
        // Display metadata
        if (!content.metadata.empty()) {
            std::cout << "\n[META] Metadata:" << std::endl;
            for (const auto& [key, value] : content.metadata) {
                std::cout << "   " << key << ": " << value << std::endl;
            }
        }
        
        // Display files with comprehensive information
        std::cout << "\n[FILES] Files Retrieved:" << std::endl;
        size_t total_size = 0;
        int file_count = 0;
        int dir_count = 0;
        
        for (size_t i = 0; i < content.files.size(); ++i) {
            const auto& file = content.files[i];
            
            // Determine if this is a directory or file
            bool is_directory = file.content.empty() && !file.path.empty() && file.path.back() == '/';
            if (is_directory) {
                dir_count++;
                std::cout << "\n  [DIR " << dir_count << "] ";
            } else {
                file_count++;
                std::cout << "\n  [FILE " << file_count << "] ";
            }
            
            if (file.path.empty()) {
                std::cout << "(root content)";
            } else {
                std::cout << file.path;
            }
            
            std::cout << std::endl;
            
            if (!is_directory) {
                std::cout << "      Size: " << file.content.size() << " bytes";
                total_size += file.content.size();
                
                if (file.size != file.content.size()) {
                    std::cout << " (declared: " << file.size << ")";
                }
                std::cout << std::endl;
            }
            
            if (file.mode) {
                std::cout << "      Mode: 0" << std::oct << file.mode.value() << std::dec << std::endl;
            }
            
            if (file.mtime) {
                std::cout << "      Modified: " << file.mtime->seconds();
                if (file.mtime->has_fractionalnanoseconds()) {
                    std::cout << "." << file.mtime->fractionalnanoseconds();
                }
                std::cout << std::endl;
            }
            
            // Show content preview for small non-directory files
            if (!is_directory && file.content.size() > 0 && file.content.size() <= 200) {
                std::cout << "      Preview: ";
                bool is_binary = false;
                for (char c : file.content) {
                    if (c < 32 && c != '\n' && c != '\t' && c != '\r') {
                        is_binary = true;
                        break;
                    }
                }
                
                if (is_binary) {
                    std::cout << "[Binary data]" << std::endl;
                } else {
                    std::string preview(file.content.begin(), file.content.end());
                    if (preview.length() > 100) {
                        preview = preview.substr(0, 97) + "...";
                    }
                    std::cout << "\"" << preview << "\"" << std::endl;
                }
            }
        }
        
        std::cout << "\n[SUMMARY] Download Summary:" << std::endl;
        std::cout << "==============================" << std::endl;
        std::cout << "[STATS] Files downloaded: " << file_count << std::endl;
        std::cout << "[STATS] Directories found: " << dir_count << std::endl;
        std::cout << "[STATS] Total content size: " << total_size << " bytes";
        
        // Convert to human-readable format
        if (total_size > 1024 * 1024) {
            std::cout << " (" << std::fixed << std::setprecision(2) << (total_size / (1024.0 * 1024.0)) << " MB)";
        } else if (total_size > 1024) {
            std::cout << " (" << std::fixed << std::setprecision(2) << (total_size / 1024.0) << " KB)";
        }
        std::cout << std::endl;
        
        // Show directory structure if we have multiple files
        if (content.files.size() > 1 && dir_count > 0) {
            std::cout << "\n[STRUCTURE] Directory Structure:" << std::endl;
            for (const auto& file : content.files) {
                if (!file.path.empty()) {
                    std::cout << "  " << file.path;
                    if (file.content.empty() && file.path.back() == '/') {
                        std::cout << " [DIR]";
                    } else {
                        std::cout << " (" << file.content.size() << " bytes)";
                    }
                    std::cout << std::endl;
                }
            }
        }
        
        std::cout << "\n[SUCCESS] Content retrieval test completed successfully!" << std::endl;
    }
    
    void run() {
        std::cout << "[TEST] IPFS Bitswap Content Retrieval Test" << std::endl;
        std::cout << "=====================================" << std::endl;
        
        // Test parameters
        const std::string ipfs_node = "/ip4/192.168.46.124/tcp/4001/p2p/12D3KooWHsD2QEUS5FzHEyq2bTuwMSEuEvV86wVAc7VaDDKK1NwJ";
        const std::string test_cid = "QmdHvvEXRUgmyn1q3nkQwf9yE412Vzy5gSuGAukHRLicXA";
        
        // Connect to IPFS node
        if (!connectToIPFSNode(ipfs_node)) {
            std::cerr << "[ERROR] Failed to connect to IPFS node" << std::endl;
            return;
        }
        
        // Test content retrieval
        testContentRetrieval(test_cid);
        
        std::cout << "\n[COMPLETE] Test execution completed." << std::endl;
    }
    
private:
    std::optional<libp2p::peer::PeerInfo> peer_info_;
};

int main() {
    try {
        BitswapContentTest test;
        test.run();
        
        // Add a small delay to let any remaining downloads complete
        std::cout << "\n[WAIT] Waiting for any remaining operations to complete..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        test.shutdown(); // Explicit shutdown before destructor
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[FAILED] Test failed with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "[FAILED] Test failed with unknown exception" << std::endl;
        return 1;
    }
}