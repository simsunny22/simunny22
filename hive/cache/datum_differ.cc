#include "hive/cache/datum_differ.hh"
#include <ostream>
#include <seastar/core/sstring.hh>

namespace hive {

std::ostream& operator<<(std::ostream& os, const datum_differ& differ) {
    os << "datum_differ";
    
    return os;
}

}//namespace hive
