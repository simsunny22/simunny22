#pragma once

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>
#include "bytes_ostream.hh"
#include "hive/hive_request.hh"
#include "types.hh"
#include "bytes.hh"

namespace hive{
class extent_datum;

class hive_result_digest{
public:
    static_assert(16 == CryptoPP::Weak::MD5::DIGESTSIZE, "MD5 digest size is all wrong");
    using type = std::array<uint8_t, 16>;
private:
    type _digest;
public:
    hive_result_digest() = default;
    hive_result_digest(type&& digest) : _digest(std::move(digest)) {}
    const type& get() const { return _digest; }
    bool operator==(const hive_result_digest& rh) const {
        return _digest == rh._digest;
    }
    bool operator!=(const hive_result_digest& rh) const {
        return _digest != rh._digest;
    }
};

class hive_result {
    bytes_ostream _w;
    sstring _hit_disk;
    
public:

    hive_result() {}
    hive_result(bytes_ostream&& w) : _w(std::move(w)) {}
    hive_result(bytes_ostream&& w, sstring hit_disk) : _w(std::move(w))
                                                     , _hit_disk(hit_disk){}
    hive_result(hive::extent_datum&& extent);
    hive_result(hive_result&&) = default;
    hive_result(const hive_result&) = default;
    hive_result& operator=(hive_result&&) = default;
    hive_result& operator=(const hive_result&) = default;

    const bytes_ostream& buf() const {
        return _w;
    }
    
    const sstring& hit_disk() const{
        return _hit_disk;
    }

    hive_result_digest digest() {
        CryptoPP::Weak::MD5 hash;
        hive_result_digest::type digest;
        bytes_view v = _w.linearize();
        hash.CalculateDigest(reinterpret_cast<unsigned char*>(digest.data()), reinterpret_cast<const unsigned char*>(v.begin()), v.size());
        return hive_result_digest(std::move(digest));
    }

    sstring md5() {
        CryptoPP::Weak::MD5 hash;
        bytes digest(bytes::initialized_later(), 16);
        std::memset(digest.begin(), 0, 16);
        bytes_view v = _w.linearize();
        hash.CalculateDigest(reinterpret_cast<unsigned char*>(digest.begin())
                           , reinterpret_cast<const unsigned char*>(v.begin())
                           , v.size());

        return to_hex(digest);  
    }
};

} //namespace hive
