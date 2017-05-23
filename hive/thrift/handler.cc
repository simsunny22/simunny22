/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

// Some thrift headers include other files from within namespaces,
// which is totally broken.  Include those files here to avoid
// breakage:
#include <sys/param.h>
// end thrift workaround
#include "Hive.h"
#include "types.hh"
#include "exceptions/exceptions.hh"
#include "core/distributed.hh"
#include "core/sstring.hh"
#include "core/print.hh"
#include "utils/UUID_gen.hh"
#include "utils/class_registrator.hh"
#include <thrift/protocol/TBinaryProtocol.h>
#include <boost/move/iterator.hpp>
#include "noexcept_traits.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/uniqued.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/range/adaptor/indirected.hpp>

#include "hive/thrift/utils.hh"
#include "hive/thrift/thrift_validation.hh"
#include "hive/thrift/server.hh"

#include "hive/hive_request.hh"
#include "hive/stream_service.hh" 
#include "hive/volume_driver.hh"
#include "hive/volume_service.hh"

#include "log.hh"

static logging::logger logger("hive-thrift-handler");

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::async;

using namespace ::org::apache::hive;

using namespace hive_thrift;

class hive_unimplemented_exception : public std::exception {
public:
    virtual const char* what() const throw () override { return "sorry, not implemented"; }
};

void hive_pass_unimplemented(const tcxx::function<void(::apache::thrift::TDelayedException* _throw)>& exn_cob) {
    exn_cob(::apache::thrift::TDelayedException::delayException(hive_unimplemented_exception()));
}

class hive_delayed_exception_wrapper : public ::apache::thrift::TDelayedException {
    std::exception_ptr _ex;
public:
    hive_delayed_exception_wrapper(std::exception_ptr ex) : _ex(std::move(ex)) {}
    virtual void throw_it() override {
        // Thrift auto-wraps unexpected exceptions (those not derived from TException)
        // with a TException, but with a fairly bad what().  So detect this, and
        // provide our own TException with a better what().
        try {
            std::rethrow_exception(std::move(_ex));
        } catch (const ::apache::thrift::TException&) {
            // It's an expected exception, so assume the message
            // is fine.  Also, we don't want to change its type.
            throw;
        } catch (no_such_class& nc) {
            throw make_exception<InvalidRequestException>(nc.what());
        } catch (marshal_exception& me) {
            throw make_exception<InvalidRequestException>(me.what());
        } catch (exceptions::already_exists_exception& ae) {
            throw make_exception<InvalidRequestException>(ae.what());
        } catch (exceptions::configuration_exception& ce) {
            throw make_exception<InvalidRequestException>(ce.what());
        } catch (exceptions::invalid_request_exception& ire) {
            throw make_exception<InvalidRequestException>(ire.what());
        } catch (exceptions::syntax_exception& se) {
            throw make_exception<InvalidRequestException>("syntax error: %s", se.what());
        } catch (std::exception& e) {
            // Unexpected exception, wrap it
            throw ::apache::thrift::TException(std::string("Internal server error: ") + e.what());
        } catch (...) {
            // Unexpected exception, wrap it, unfortunately without any info
            throw ::apache::thrift::TException("Internal server error");
        }
    }
};

template <typename Func, typename T>
void
hive_with_cob(tcxx::function<void (const T& ret)>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<noexcept_movable_t<T>>::apply([func = std::forward<Func>(func)] {
        return noexcept_movable<T>::wrap(func());
    }).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (auto&& f) {
        try {
            cob(noexcept_movable<T>::unwrap(f.get0()));
        } catch (...) {
            hive_delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

template <typename Func>
void
hive_with_cob(tcxx::function<void ()>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<void>::apply(func).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<> f) {
        try {
            f.get();
            cob();
        } catch (...) {
            hive_delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

template <typename Func>
void
with_exn_cob(tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob, Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<void>::apply(func).then_wrapped([exn_cob = std::move(exn_cob)] (future<> f) {
        try {
            f.get();
        } catch (...) {
            hive_delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
       }
    });
}

std::string hive_bytes_to_string(bytes_view v) {
    return { reinterpret_cast<const char*>(v.begin()), v.size() };
}

class hive_thrift_handler : public HiveCobSvIf {
private:
public:
    explicit hive_thrift_handler()
    {}

    void ping(tcxx::function<void(std::string const& _return)> cob, const std::string& msg) {
//std::cout << "hive thrift handler ping msg: " << msg << std::endl;
        cob("pong");
    }

    void volume_write(tcxx::function<void(int64_t const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& volumeId, const int64_t offset, const int64_t length, const std::string& data) {
        with_exn_cob(std::move(exn_cob), [&] {
//std::cout << "hive thrift handler write msg: " << volumeId << std::endl;
            hive::hive_write_command cmd(sstring(volumeId), offset, length, bytes(reinterpret_cast<const signed char *>(data.c_str()), data.length()));
//std::cout << "hive thrift handler hive write cmd: " << cmd << std::endl;

            return bind_volume_driver(sstring(volumeId)).then([volumeId] (...) {
                auto& stream_service = hive::get_local_stream_service();
//std::cout << "hive thrift handler hive write find stream volume id: " << volumeId << std::endl;
                return stream_service.find_stream(volumeId);
            }).then([write_cmd=std::move(cmd), volumeId](auto stream) mutable {
                logger.debug("[rpc_rwrite_volume] start, write_cmd:{}", write_cmd);
//std::cout << "hive thrift handler hive write find stream volume id: " << volumeId << std::endl;
                return stream->rwrite_volume(std::move(write_cmd)); 
            }).then([cob=std::move(cob), &volumeId, &offset, &length] () {
//std::cout << "hive thrift handler hive write success" << std::endl;
                return cob(length);
            });
        });
    }

    void volume_read(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& volumeId, const int64_t offset, const int64_t length) {
        with_exn_cob(std::move(exn_cob), [&] {
//std::cout << "hive thrift handler read msg: " << volumeId << std::endl;
            hive::hive_read_command cmd(sstring(volumeId), offset, length);
//std::cout << "hive thrift handler hive read cmd: " << cmd << std::endl;

            return bind_volume_driver(sstring(volumeId)).then([volumeId] (...) {
                auto& stream_service = hive::get_local_stream_service();
                return stream_service.find_stream(volumeId);
            }).then([read_cmd=std::move(cmd)](auto stream) mutable {
                logger.debug("[rpc_read_volume] start, read_cmd:{}", read_cmd);
//std::cout << "hive thrift handler hive read find stream" << std::endl;
                return stream->read_volume(std::move(read_cmd)); 
            }).then([cob=std::move(cob)] (bytes data_) {
//std::cout << "hive thrift handler hive read data length: " << data_.length() << std::endl;
                return cob(hive_bytes_to_string(data_));
            });
        });
    }


private:
    future<hive::driver_info> bind_volume_driver(sstring volumeId) {
        auto shard = hive::get_local_volume_service().shard_of(volumeId);
        return hive::get_volume_service().invoke_on(shard, [volumeId] (auto& volume_service) {
            return volume_service.bind_volume_driver(volumeId);
        });
    }
};

class hive_handler_factory : public HiveCobSvIfFactory {
public:
    explicit hive_handler_factory() {}
    typedef HiveCobSvIf Handler;
    virtual HiveCobSvIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) {
        return new hive_thrift_handler();
    }
    virtual void releaseHandler(HiveCobSvIf* handler) {
        delete handler;
    }
};

std::unique_ptr<HiveCobSvIfFactory>
create_hive_handler_factory() {
    return std::make_unique<hive_handler_factory>();
}
