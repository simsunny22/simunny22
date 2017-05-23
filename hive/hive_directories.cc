#include "hive/hive_directories.hh"
#include "log.hh"


namespace hive {

static logging::logger logger("hive_directories");

future<> directories::touch_and_lock(sstring path){
    return io_check(recursive_touch_directory, path).then_wrapped([this, path] (future<> f) {
        try {
            f.get();
            return utils::file_lock::acquire(path + "/.lock").then([this](utils::file_lock lock) {
               _locks.emplace_back(std::move(lock));
            }).handle_exception([path](auto ep) {
                // only do this because "normal" unhandled exception exit in seastar
                // _drops_ system_error message ("what()") and thus does not quite deliver
                // the relevant info to the user
                try {
                    std::rethrow_exception(ep);
                } catch (...) {
                    logger.error("[touch_and_lock] error, initialize {}: {}", path, std::current_exception());
                    throw;
                }
            });
        } catch (std::system_error& e) {
            logger.error("[touch_and_lock] error, {} not found. Tried to created it but failed: {}", path, e.what());
            throw;
        }
    });

}

} //namespace hive 
