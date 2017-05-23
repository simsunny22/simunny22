#include "seawreck.hh"

#include "seastar/core/print.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/distributed.hh"
#include "seastar/core/semaphore.hh"
#include "seastar/core/thread.hh"

#include "hive/http/hive_http_response_parser.hh"

#include <chrono>
#include <iostream>


namespace hive {


template <typename... Args>
void http_debug(const char* fmt, Args&&... args) {
//#if HTTP_DEBUG
    print(fmt, std::forward<Args>(args)...);
//#endif
}    

HttpClient::HttpClient() {
   _parser = std::make_unique<hive_response_parser>();
	_write_buffer = output_stream<char>{};
	_read_buffer  = input_stream<char>{};
}

HttpClient::~HttpClient() {
//print("HttpClient delete ----------- \n");
}

HttpClient::HttpClient(HttpClient&& other) {
	*this = std::move(other);
}

HttpClient& HttpClient::operator=(HttpClient&& other) {
    if(this != &other) {
      _parser = std::move(other._parser);
    	_socket = std::move(other._socket);
    	_write_buffer = std::move(other._write_buffer);
    	_read_buffer  = std::move(other._read_buffer);
	}

	return *this;
}

void HttpClient::reset() {
    _write_buffer = output_stream<char>{};
    _read_buffer = input_stream<char>{};
}


future<> HttpClient::do_connect(ipv4_addr server_addr) {
    //print("=====do_connect==============>%s\n", server_addr);
    // Establish all the TCP connections first
    return engine().net().connect(server_addr).then([this] (auto socket) mutable {
		    this->_socket = std::move(socket);
		    this->_read_buffer  = std::move(_socket.input());
		    _write_buffer = std::move(_socket.output());
		    return make_ready_future<>();
    });
}

future<> HttpClient::do_write(sstring str) {
    return _write_buffer.write(str).then([this] {
        return _write_buffer.flush();
    });
}

int get_content_length(auto& _rsp){
    auto it = _rsp->_headers.find("Content-Length");
    if(it == _rsp->_headers.end()){
        it = _rsp->_headers.find("content-length");
    }

    if(it != _rsp->_headers.end()){
        auto content_length = std::stoi(it->second);
        return content_length; 
    }else{
        throw std::runtime_error("Error: HTTP response does not contain: Content-Length or content-length"); 
    }
}

future<hive::Response> HttpClient::do_read() {
    _parser->init();
    return _read_buffer.consume(*_parser).then([this] {
        // Read HTTP response header first
	      hive::Response response;
        if (_parser->eof()) {
            throw std::runtime_error("HttpClient::do_read response is empty");
        }
        auto _rsp = _parser->get_parsed_response();
        response.statusIs((hive::Response::Status) _rsp->_status_code);

        for(auto header : _rsp->_headers) {
		        response.headerIs(header.first, header.second); 
        }
	
        // Read HTTP response body
		    auto content_length = get_content_length(_rsp);
		    //if (-1==content_length) {
        //    return make_ready_future<hive::Response>(std::move(response));
		    //}
        return _read_buffer.read_exactly(content_length).then([response=std::move(response), content_length]
                (auto&& content) mutable{
		        sstring body(content.begin(), content_length); 
		        response.dataIs(body); 
            return make_ready_future<hive::Response>(std::move(response));
		    });
    });
}

sstring HttpClient::sstr(hive::Request::Method method) {
    switch (method) {
    case hive::Request::GET:     return "GET";
    case hive::Request::HEAD:    return "HEAD";
    case hive::Request::POST:    return "POST";
    case hive::Request::PUT:     return "PUT";
    case hive::Request::DELETE:  return "DELETE";
    case hive::Request::TRACE:   return "TRACE";
    case hive::Request::CONNECT: return "CONNECT";
    default: assert(!"unknown request method");
    }
    return "";
}

sstring HttpClient::sstr(hive::Request const& request) {
    // Serialize a request to sstring
    std::stringstream ss;
    auto path = request.path().empty() ? "/" : request.path();
    ss << sstr(request.method()) << ' ' << path << " HTTP/1.1\r\n";
    ss << hive::Headers::HOST << ": " << request.uri().host() << "\r\n";
    ss << hive::Headers::CONTENT_LENGTH << ": " << request.data().size() << "\r\n";
    ss << hive::Headers::CONNECTION << ": close\r\n";
    ss << hive::Headers::ACCEPT_ENCODING << ": identity\r\n";
    for(auto header : request.headers()) {
        ss << header.first << ": " << header.second << "\r\n";
    }
    ss << "\r\n";
    ss << request.data();
    return ss.str();
}

future<hive::Response> HttpClient::send(hive::Request const& request) {
    sstring  host = request.uri().host(); 
    uint16_t port = request.uri().port();
    ipv4_addr server_addr(host, port);

    sstring str = std::move(sstr(request) );
    return seastar::async([this, server_addr, str=std::move(str)] {
        do_connect(server_addr).get0();
        do_write(str).get0();
        auto reply = do_read().get0();
        return std::move(reply);
	});
}

future<hive::Response> HttpClient::get(sstring const& path, sstring const& data) {
    // Shortcut for simple GET requests
    hive::Request request;
    request.methodIs(hive::Request::GET);
    request.uriIs(hive::Uri(path));
    request.dataIs(data);
    return send(request);
}

future<hive::Response> HttpClient::post(sstring const& path, sstring const& data) {
    // Shortcut for simple POST requests
    hive::Request request;
    request.methodIs(hive::Request::POST);
    request.uriIs(hive::Uri(path));
    request.dataIs(data);
    return send(request);
}

} // namespace hive
