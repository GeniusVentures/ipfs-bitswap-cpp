// Deterministic race-ordering tests for BitswapRequestContext timer semantics.
//
// SC4 contract: every test runs on a single-threaded io_context and drives
// timer orderings with injected durations (zero-millisecond / long) plus
// poll()-family stepping — no wall-clock waits, no worker threads, no sleeps.
// Repeated runs produce identical callback counts.
//
//   ordering 1: long duration; AddCallback(cb); HandleResponse(success);
//               poll() to exhaustion -> aborted timer handler no-ops (BSTP-01)
#include <gtest/gtest.h>

#include <boost/asio.hpp>

#include <memory>
#include <string>

#include "../src/bitswap.hpp"

using namespace sgns::ipfs_bitswap;

/**
 * @given a BitswapRequestContext armed with a long (60 s) timeout and one
 *        registered callback
 * @when the request completes via HandleResponse(success) and the io_context
 *       is then drained so the aborted timer handler provably runs
 * @then exactly one success callback is delivered and no error callback ever
 *       fires — a canceled timer never behaves as a timeout (BSTP-01)
 */
TEST( RequestContextRaceTest, CancelBeforeExpiryYieldsExactlyOneSuccess )
{
    boost::asio::io_context io;

    auto context = std::make_shared<BitswapRequestContext>(
        io, boost::posix_time::seconds( 60 ) );

    int successCount = 0;
    int errorCount   = 0;

    context->AddCallback( [&successCount, &errorCount](
                              libp2p::outcome::result<std::string> result ) {
        if ( result )
        {
            ++successCount;
        }
        else
        {
            ++errorCount;
        }
    } );

    context->HandleResponse( std::string( "block-data" ) );

    // Drain the context so the aborted timer handler provably executes —
    // it must return through the ec guard without firing anything.
    while ( io.poll() > 0 )
    {
    }

    EXPECT_EQ( successCount, 1 );
    EXPECT_EQ( errorCount, 0 );
}
