#include "hive/stream/progress_info.hh"

namespace hive {

std::ostream& operator<<(std::ostream& os, const progress_info& x) {
    sstring dir = x.dir == progress_info::direction::OUT ? "sent to " : "received from ";
    return os << sprint("%s %ld/(%f\%) %s %s", x.file_name, x.current_bytes,
            x.current_bytes * 100 / x.total_bytes, dir, x.peer);
}

}//namespace hive
