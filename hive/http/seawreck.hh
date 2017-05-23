#ifndef __SEAWRECK_H__
#define __SEAWRECK_H__

#include "seastar/core/temporary_buffer.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/semaphore.hh"
#include <chrono>
#include "hive/http/request.hh"
#include "hive/http/response.hh"


class hive_response_parser;


namespace hive {

// class HttpClient
class HttpClient {
private:
	  std::unique_ptr<hive_response_parser> _parser;

	  connected_socket _socket;
    input_stream<char>  _read_buffer;
    output_stream<char> _write_buffer;

	  future<hive::Response> send(hive::Request const& request);
	  future<> do_connect(ipv4_addr server_addr);
	  future<> do_write(sstring str);
	  future<hive::Response> do_read();

    sstring sstr(hive::Request::Method method);
    sstring sstr(hive::Request const& request);
public:
    HttpClient();
	  ~HttpClient();

	  void reset();

    HttpClient(HttpClient&& other);
	  HttpClient & operator=(HttpClient&& other);

    future<hive::Response> get(sstring const& path, sstring const& data="");
    future<hive::Response> post(sstring const& path, sstring const& data="");
};

} // namespace http

#endif //__SEAWRECK_H__
