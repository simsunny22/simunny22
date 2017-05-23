#include "hive/stream/test_message.hh"
#include <ostream>
#include <seastar/core/sstring.hh>

namespace hive {

std::ostream& operator<<(std::ostream& os, const test_message& chunk) {
    os << "test_message";
    
    return os;
}

}//namespace hive
