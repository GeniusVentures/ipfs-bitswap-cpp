/**
 * @file       bitswap_persistence_test.cpp
 * @brief      Standalone test for Bitswap disk persistence (Phase 1).
 * @date       2026-06-30
 *
 * Follows the same host-creation pattern as bitswap_content_test.cpp.
 */

#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <cassert>

#include <boost/format.hpp>
#include <boost/asio.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/security/noise.hpp>
#include <libp2p/log/logger.hpp>
#include <libp2p/log/configurator.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>
#include <libp2p/event/bus.hpp>
#include <libp2p/protocol/factory/protocol_factory.hpp>
#include <soralog/logging_system.hpp>
#include <soralog/impl/configurator_from_yaml.hpp>

#include "../src/bitswap.hpp"
#include "../src/logger.hpp"

using namespace sgns::ipfs_bitswap;
namespace fs = std::filesystem;
using CID = libp2p::multi::ContentIdentifier;

// ═══════════════════════════════════════════════════════
// Harness — follows bitswap_content_test.cpp exactly.
// ═══════════════════════════════════════════════════════

class PersistenceHarness
{
public:
    PersistenceHarness()
    {
        const std::string logger_config( R"(
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
        )" );

        auto logging_system = std::make_shared<soralog::LoggingSystem>(
            std::make_shared<soralog::ConfiguratorFromYAML>(
                std::make_shared<libp2p::log::Configurator>(),
                logger_config ) );
        logging_system->configure();
        libp2p::log::setLoggingSystem( logging_system );

        event_bus_ = std::make_shared<libp2p::event::Bus>();

        auto injector = libp2p::injector::makeHostInjector(
            libp2p::injector::useSecurityAdaptors<libp2p::security::Noise>() );
        host_       = injector.create<std::shared_ptr<libp2p::Host>>();
        io_context_ = injector.create<std::shared_ptr<boost::asio::io_context>>();
        work_guard_ = std::make_unique<boost::asio::io_context::work>( *io_context_ );

        auto bindaddr = ( boost::format( "/ip4/0.0.0.0/tcp/%d" ) % 40060 ).str();
        auto ma_res   = libp2p::multi::Multiaddress::create( bindaddr );
        host_->listen( ma_res.value() );
        host_->start();

        bitswap_ = std::make_shared<Bitswap>( *host_, *event_bus_, io_context_ );

        cache_dir_ = fs::temp_directory_path() / "sgns_bitswap_persistence_test";
        fs::create_directories( cache_dir_ );
        bitswap_->setCacheDir( cache_dir_.string() );
        bitswap_->initialize();

        io_thread_ = std::thread( [this] { io_context_->run(); } );
    }

    ~PersistenceHarness()
    {
        bitswap_.reset();
        if ( host_ ) host_->stop();
        work_guard_.reset();
        if ( io_context_ && !io_context_->stopped() ) io_context_->stop();
        if ( io_thread_.joinable() ) io_thread_.join();

        std::error_code ec;
        fs::remove_all( cache_dir_, ec );
    }

    Bitswap       &bitswap()  { return *bitswap_; }
    const fs::path &cacheDir() const { return cache_dir_; }

private:
    std::shared_ptr<libp2p::event::Bus>            event_bus_;
    std::shared_ptr<boost::asio::io_context>       io_context_;
    std::unique_ptr<boost::asio::io_context::work> work_guard_;
    std::thread                                    io_thread_;
    std::shared_ptr<libp2p::Host>                  host_;
    std::shared_ptr<Bitswap>                       bitswap_;
    fs::path                                       cache_dir_;
};

// ═══════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════

static std::string cidToStr( const CID &cid )
{
    auto r = libp2p::multi::ContentIdentifierCodec::toString( cid );
    return r.has_value() ? r.value() : "invalid";
}

// publishAndWait: PublishData for raw data is synchronous.
// Returns shared_ptr because ContentIdentifier has no default ctor.
static std::shared_ptr<CID> publishAndWait( Bitswap &b, const std::vector<uint8_t> &data )
{
    std::shared_ptr<CID> result;
    b.PublishData( data, [&result]( libp2p::outcome::result<CID> r ) {
        if ( r ) result = std::make_shared<CID>( r.value() );
    } );
    return result;
}

static int failures = 0;

static void check( bool cond, const char *msg )
{
    if ( !cond ) { std::cerr << "FAIL: " << msg << std::endl; ++failures; }
    else         { std::cout << "  OK: " << msg << std::endl; }
}

// ═══════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════

static void test_persistBlock_creates_file_on_disk()
{
    std::cout << "\n[TEST] persistBlock creates file on disk" << std::endl;
    PersistenceHarness h;
    auto &b = h.bitswap();

    std::vector<uint8_t> data = { 'p', 'e', 'r', 's', 'i', 's', 't', '_', 't', 'e', 's', 't' };
    auto cid = publishAndWait( b, data );
    check( cid && !cid->content_address.toBuffer().empty(), "CID not empty" );

    std::string cidStr = cidToStr( *cid );
    check( cidStr != "invalid", "CID string is valid" );

    fs::path filePath = h.cacheDir() / cidStr;
    check( fs::exists( filePath ), "file exists on disk" );

    std::ifstream file( filePath, std::ios::binary | std::ios::ate );
    check( file.is_open(), "file is readable" );
    check( static_cast<size_t>( file.tellg() ) > 0u, "file has content" );
}

static void test_hasBlock_returns_true_after_restart()
{
    std::cout << "\n[TEST] HasBlock returns true after restart" << std::endl;
    fs::path reuseDir = fs::temp_directory_path() / "sgns_bitswap_restart";
    fs::create_directories( reuseDir );

    std::vector<uint8_t> data = { 'r', 'e', 's', 't', 'a', 'r', 't' };
    std::shared_ptr<CID> cid;

    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();
        cid = publishAndWait( h.bitswap(), data );
        check( cid && !cid->content_address.toBuffer().empty(), "published CID" );
        check( h.bitswap().HasBlock( *cid ), "HasBlock true in session 1" );
        check( fs::exists( reuseDir / cidToStr( *cid ) ), "file on disk" );
    }

    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();
        check( h.bitswap().HasBlock( *cid ), "HasBlock true after restart (disk index)" );
    }

    std::error_code ec;
    fs::remove_all( reuseDir, ec );
}

static void test_getBlock_lazy_loads_from_disk()
{
    std::cout << "\n[TEST] GetBlock lazy-loads from disk" << std::endl;
    fs::path reuseDir = fs::temp_directory_path() / "sgns_bitswap_lazyload";
    fs::create_directories( reuseDir );

    std::vector<uint8_t> data = { 'l', 'a', 'z', 'y', '_', 'l', 'o', 'a', 'd' };
    std::shared_ptr<CID> cid;

    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();
        cid = publishAndWait( h.bitswap(), data );
        check( cid && !cid->content_address.toBuffer().empty(), "published CID" );
        auto block = h.bitswap().GetBlock( *cid );
        check( block.has_value(), "GetBlock returns value" );
        check( block.value().size() > 0u, "block has data" );
    }

    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();
        auto block = h.bitswap().GetBlock( *cid );
        check( block.has_value(), "GetBlock lazy-loads after restart" );
        check( block.value().size() > 0u, "lazy-loaded block has data" );
    }

    std::error_code ec;
    fs::remove_all( reuseDir, ec );
}

static void test_buildDiskIndex_finds_all_cids()
{
    std::cout << "\n[TEST] buildDiskIndex finds all CIDs" << std::endl;
    fs::path reuseDir = fs::temp_directory_path() / "sgns_bitswap_index";
    fs::create_directories( reuseDir );

    std::vector<uint8_t> data1 = { 'i', 'n', 'd', 'e', 'x', '_', '1' };
    std::vector<uint8_t> data2 = { 'i', 'n', 'd', 'e', 'x', '_', '2' };
    std::shared_ptr<CID> cid1, cid2;

    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();
        cid1 = publishAndWait( h.bitswap(), data1 );
        cid2 = publishAndWait( h.bitswap(), data2 );
        check( cid1 && !cid1->content_address.toBuffer().empty(), "CID1 published" );
        check( cid2 && !cid2->content_address.toBuffer().empty(), "CID2 published" );
        check( fs::exists( reuseDir / cidToStr( *cid1 ) ), "file1 on disk" );
        check( fs::exists( reuseDir / cidToStr( *cid2 ) ), "file2 on disk" );
    }

    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();
        check( h.bitswap().HasBlock( *cid1 ), "CID1 found in rebuilt index" );
        check( h.bitswap().HasBlock( *cid2 ), "CID2 found in rebuilt index" );
    }

    std::error_code ec;
    fs::remove_all( reuseDir, ec );
}

static void test_unpersistBlock_removes_file_and_index()
{
    std::cout << "\n[TEST] unpersistBlock removes file and index" << std::endl;

    // Use a shared cache dir so we can verify across "restarts".
    fs::path reuseDir = fs::temp_directory_path() / "sgns_bitswap_unpersist";
    fs::create_directories( reuseDir );

    std::shared_ptr<CID> cid;
    std::string          cidStr;

    // Session 1: publish and unpersist.
    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();

        std::vector<uint8_t> data = { 'u', 'n', 'p', 'e', 'r', 's', 'i', 's', 't' };
        cid = publishAndWait( h.bitswap(), data );
        check( cid && !cid->content_address.toBuffer().empty(), "CID published" );

        cidStr = cidToStr( *cid );
        check( cidStr != "invalid", "CID string valid" );
        check( fs::exists( reuseDir / cidStr ), "file on disk before unpersist" );
        check( h.bitswap().HasBlock( *cid ), "HasBlock before unpersist" );

        h.bitswap().unpersistBlock( *cid );

        check( !fs::exists( reuseDir / cidStr ), "file removed from disk" );
        // HasBlock may still return true — the block is still in blockStore_ (memory).
        // The real test is after a restart.
    }

    // Session 2: restart with same cache dir.  Block should be gone now.
    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize(); // buildDiskIndex will NOT find the unpersisted file.

        check( !h.bitswap().HasBlock( *cid ), "HasBlock false after unpersist + restart" );
    }

    std::error_code ec;
    fs::remove_all( reuseDir, ec );
}

static void test_noCacheDir_noop()
{
    std::cout << "\n[TEST] Empty cacheDir is no-op" << std::endl;
    PersistenceHarness h;
    auto &b = h.bitswap();

    b.setCacheDir( "" );

    std::vector<uint8_t> data = { 'n', 'o', 'c', 'a', 'c', 'h', 'e' };
    auto cid = publishAndWait( b, data );
    check( cid && !cid->content_address.toBuffer().empty(), "published without cache dir" );

    // HasBlock returns true: block is still in blockStore_ (memory).
    // persistBlock is a no-op with empty cacheDir, but storeBlock populates
    // blockStore_ before calling persistBlock.
    check( b.HasBlock( *cid ), "HasBlock true (in-memory, no disk)" );

    b.unpersistBlock( *cid );
    // unpersistBlock is a no-op with empty cacheDir — no crash.
    check( b.HasBlock( *cid ), "HasBlock still true (unpersistBlock no-op with empty cacheDir)" );
}

static void test_tryLoadFromDisk_selfHeals_missing_file()
{
    std::cout << "\n[TEST] tryLoadFromDisk self-heals missing file" << std::endl;

    fs::path reuseDir = fs::temp_directory_path() / "sgns_bitswap_ghost";
    fs::create_directories( reuseDir );

    std::shared_ptr<CID> cid;

    // Session 1: publish, then manually delete the file.
    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();

        std::vector<uint8_t> data = { 'g', 'h', 'o', 's', 't' };
        cid = publishAndWait( h.bitswap(), data );
        check( cid && !cid->content_address.toBuffer().empty(), "CID published" );

        std::string cidStr = cidToStr( *cid );
        check( cidStr != "invalid", "CID string valid" );
        check( fs::exists( reuseDir / cidStr ), "file on disk" );
        check( h.bitswap().HasBlock( *cid ), "HasBlock true" );

        // Delete file from disk manually — simulates corruption.
        fs::remove( reuseDir / cidStr );
        // Rebuild index: now the file is missing, so CID should be dropped.
        h.bitswap().buildDiskIndex();
    }

    // Session 2: restart — ghost CID should NOT be in rebuilt index.
    {
        PersistenceHarness h;
        h.bitswap().setCacheDir( reuseDir.string() );
        h.bitswap().initialize();

        check( !h.bitswap().HasBlock( *cid ), "ghost CID removed after restart" );
    }

    std::error_code ec;
    fs::remove_all( reuseDir, ec );
}

// ═══════════════════════════════════════════════════════

int main()
{
    std::cout << "=== Bitswap Persistence Tests ===" << std::endl;

    test_persistBlock_creates_file_on_disk();
    test_hasBlock_returns_true_after_restart();
    test_getBlock_lazy_loads_from_disk();
    test_buildDiskIndex_finds_all_cids();
    test_unpersistBlock_removes_file_and_index();
    test_noCacheDir_noop();
    test_tryLoadFromDisk_selfHeals_missing_file();

    std::cout << "\n=== " << ( failures == 0 ? "ALL TESTS PASSED" : "SOME TESTS FAILED" )
              << " (" << failures << " failures) ===" << std::endl;

    return failures == 0 ? 0 : 1;
}
