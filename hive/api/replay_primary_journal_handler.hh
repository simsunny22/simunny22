#ifndef __HIVE_REPLAY_PRIMARY_JOURNAL_HANDLER__
#define __HIVE_REPLAY_PRIMARY_JOURNAL_HANDLER__

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <vector>
#include <unordered_map>
#include "types.hh"


namespace hive{

using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;
using request_params = std::map<sstring, data_value>;

class replay_primary_journal_handler: public httpd::handler_base {
private:
    lw_shared_ptr<request_params> parse_write_params(request& req);
    future<> replay_commitlog(sstring volume_id);
public:
    future<std::unique_ptr<reply>> handle(const sstring& path
                                         , std::unique_ptr<request> req
                                         , std::unique_ptr<reply> rep) override;
};

}//namespace hive

#endif
