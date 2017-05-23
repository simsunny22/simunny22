#ifndef __ERROR_HH__
#define __ERROR_HH__
//#pragma once

//#include "http\Common.hpp"

namespace hive {

class Error {
public:
    Error(sstring const& what) : what_(what) {}

    sstring const& what() const { return what_; }
private:
    sstring what_;

};

} //namespace hive
#endif //__ERROR_HH__
