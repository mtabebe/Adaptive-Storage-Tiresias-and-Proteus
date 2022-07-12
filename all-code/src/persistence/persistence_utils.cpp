#include "persistence_utils.h"
// c++17 is supposed to have <filesystem> but for whatever reason when I compile
// it doesn't find the header. So I'm using the boost header, but it's
// a stable API as of c++17
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <glog/logging.h>

void create_directory( const std::string& directory ) {
    boost::system::error_code err;
    DVLOG( 5 ) << "Creating directory:" << directory;
    boost::filesystem::path dir( directory );
    boost::filesystem::create_directories( dir, err );
    if( err ) {
        LOG( FATAL ) << "Could not create directory:" << directory << " :"
                     << err.value() << " , " << err.message();
    }
    DVLOG( 5 ) << "Creating directory:" << directory << " okay!";
}
