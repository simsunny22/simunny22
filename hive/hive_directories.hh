#ifndef __HIVE_DIRECTORIES_H__
#define __HIVE_DIRECTORIES_H__

#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <db/config.hh>
#include <cstdio>
#include <database.hh>
#include "utils/file_lock.hh"
#include "disk-error-handler.hh"


namespace hive {

class directories {
public:
    future<> touch_and_lock(sstring path);

    template<typename _Iter>
    future<> touch_and_lock(_Iter i, _Iter e) {
        return parallel_for_each(i, e, [this](sstring path) {
           return touch_and_lock(std::move(path));
        });
    }
    template<typename _Range>
    future<> touch_and_lock(_Range&& r) {
        return touch_and_lock(std::begin(r), std::end(r));
    }
private:
    std::vector<utils::file_lock> _locks;
}; //class directories


} //namespace hive
#endif
