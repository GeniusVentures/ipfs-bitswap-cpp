#include <iostream>
#include <memory>
#include <chrono>
#include <thread>

#include "../src/bitswap.hpp"
#include <libp2p/host/basic_host.hpp>
#include <libp2p/event/bus.hpp>
#include <boost/asio/io_context.hpp>

using namespace sgns::ipfs_bitswap;

int main()
{
    std::cout << "Bitswap Server Example\n";
    std::cout << "======================\n\n";

    // Create required components
    auto io_context = std::make_shared<boost::asio::io_context>();

    // Note: In a real application, you would properly initialize these components
    // This is a simplified example showing the API usage

    try
    {
        // Create a bitswap instance (host and eventBus would be properly initialized in real code)
        // auto host = ...; // Initialize libp2p host
        // auto eventBus = ...; // Initialize event bus
        // auto bitswap = std::make_shared<Bitswap>(host, eventBus, io_context);

        std::cout << "Example: Publishing files to bitswap\n";
        std::cout << "------------------------------------\n\n";

        // Example 1: Publishing a single file
        std::cout << "1. Publishing a single file:\n";
        std::cout << "   bitswap->PublishFile(\"/path/to/file.txt\", [](auto result) {\n";
        std::cout << "       if (result) {\n";
        std::cout << "           auto cid = result.value();\n";
        std::cout << "           std::cout << \"File published with CID: \" << cid << std::endl;\n";
        std::cout << "       } else {\n";
        std::cout << "           std::cout << \"Error: \" << result.error().message() << std::endl;\n";
        std::cout << "       }\n";
        std::cout << "   });\n\n";

        // Example 2: Publishing a directory
        std::cout << "2. Publishing a directory:\n";
        std::cout << "   bitswap->PublishDirectory(\"/path/to/directory\", [](auto result) {\n";
        std::cout << "       if (result) {\n";
        std::cout << "           auto rootCid = result.value();\n";
        std::cout << "           std::cout << \"Directory published with root CID: \" << rootCid << std::endl;\n";
        std::cout << "       }\n";
        std::cout << "   });\n\n";

        // Example 3: Publishing raw data
        std::cout << "3. Publishing raw data:\n";
        std::cout << "   std::vector<uint8_t> data = {0x48, 0x65, 0x6c, 0x6c, 0x6f}; // \"Hello\"\n";
        std::cout << "   bitswap->PublishData(data, [](auto result) {\n";
        std::cout << "       if (result) {\n";
        std::cout << "           auto cid = result.value();\n";
        std::cout << "           std::cout << \"Data published with CID: \" << cid << std::endl;\n";
        std::cout << "       }\n";
        std::cout << "   });\n\n";

        // Example 4: Checking for blocks
        std::cout << "4. Checking for blocks and retrieving them:\n";
        std::cout << "   CID someCid = ...; // Some content identifier\n";
        std::cout << "   \n";
        std::cout << "   if (bitswap->HasBlock(someCid)) {\n";
        std::cout << "       auto blockResult = bitswap->GetBlock(someCid);\n";
        std::cout << "       if (blockResult) {\n";
        std::cout
            << "           std::cout << \"Block size: \" << blockResult.value().size() << \" bytes\" << std::endl;\n";
        std::cout << "       }\n";
        std::cout << "   }\n\n";

        // Example 5: Listing published content
        std::cout << "5. Listing published content:\n";
        std::cout << "   auto publishedList = bitswap->ListPublishedContent();\n";
        std::cout << "   for (const auto& content : publishedList) {\n";
        std::cout << "       std::cout << \"Root path: \" << content.rootPath << std::endl;\n";
        std::cout << "       std::cout << \"Root CID: \" << content.rootCID << std::endl;\n";
        std::cout << "       std::cout << \"Total size: \" << content.totalSize << \" bytes\" << std::endl;\n";
        std::cout << "       std::cout << \"Number of blocks: \" << content.blocks.size() << std::endl;\n";
        std::cout << "   }\n\n";

        // Example 6: Unpublishing content
        std::cout << "6. Unpublishing content:\n";
        std::cout << "   CID rootCidToRemove = ...; // Root CID to unpublish\n";
        std::cout << "   if (bitswap->UnpublishContent(rootCidToRemove)) {\n";
        std::cout << "       std::cout << \"Content successfully unpublished\" << std::endl;\n";
        std::cout << "   }\n\n";

        std::cout << "How the server responds to requests:\n";
        std::cout << "------------------------------------\n";
        std::cout << "When a peer sends a wantlist (requesting specific blocks):\n";
        std::cout << "1. The bitswap server receives the request in the handle() method\n";
        std::cout << "2. For each CID in the wantlist, it checks its local block store\n";
        std::cout << "3. If the block is found, it sends the block back to the requesting peer\n";
        std::cout << "4. The entire process is automatic - no manual intervention needed\n\n";

        std::cout << "UnixFS and IPLD encoding:\n";
        std::cout << "-------------------------\n";
        std::cout << "- Files are encoded as UnixFS 'File' type with proper metadata\n";
        std::cout << "- Large files are automatically chunked into 256KB blocks\n";
        std::cout << "- Directories are encoded as UnixFS 'Directory' type with links to files\n";
        std::cout << "- All content is wrapped in IPLD nodes for content-addressed storage\n";
        std::cout << "- CIDs are generated automatically using SHA-256 hashing\n\n";

        std::cout << "Integration with existing client code:\n";
        std::cout << "-------------------------------------\n";
        std::cout << "The server functionality is fully compatible with existing client code.\n";
        std::cout << "You can use RequestContent() and RequestBlock() as before, and the same\n";
        std::cout << "bitswap instance can both serve blocks to peers and request blocks from peers.\n\n";
    }
    catch ( const std::exception &e )
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Example completed successfully!" << std::endl;
    return 0;
}
