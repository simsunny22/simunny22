#pragma once
#include <cstdint>
#include <vector>
#include <set>
#include <unordered_set>
#include "mutation.hh"
#include "frozen_mutation.hh"
#include "utils/UUID.hh"

namespace hive{

struct parsed_commitlog_name{
    sstring  commitlog_id;
    sstring  volume_id;
    uint32_t cpu_id;
    uint32_t version;
    uint64_t segment_id;
};

class hive_tools {
public:
    static int32_t str_to_int32(sstring str); 
    static uint32_t str_to_uint32(sstring str); 
    static int64_t str_to_int64(sstring str); 
    static uint64_t str_to_uint64(sstring str);

    static uint64_t random(uint64_t range);

    static int64_t get_current_time();
    static int64_t get_current_time_in_ms();
    static sstring parse_extent_group_id(const mutation& m);
    static sstring parse_extent_group_id(const frozen_mutation& m);
    static sstring parse_volume_id(const mutation& m);
    static sstring parse_volume_id(const frozen_mutation& m);
    static int64_t parse_length(const mutation& m);
    static int64_t parse_length(const frozen_mutation& m);
    static bytes parse_content(const mutation& m);
    static bytes parse_content(const frozen_mutation& m);

    static bytes calculate_digest(const unsigned char* data_buf, int64_t data_size);
    static sstring calculate_md5(bytes& data);
    static sstring generate_commitlog_id(sstring volume_id);
    static sstring generate_journal_segment_id(sstring volume_id);
    static sstring digest_to_md5(std::array<uint8_t, 16> digest);
    static parsed_commitlog_name parse_commitlog_file_name(sstring commitlog_name);
    static future<std::vector<sstring>> list_files(sstring dirname);
    
    static std::vector<sstring> split_to_vector(const sstring& text, const char* separator);
    static std::unordered_set<sstring> split_to_unordered_set(const sstring& text, const char* separator);
    static std::set<sstring> split_to_set(const sstring& text, const char* separator);
    
    static std::unordered_map<sstring, sstring> upper_key(const std::unordered_map<sstring, sstring>& map_str);
    static std::unordered_map<sstring, sstring> lower_key(const std::unordered_map<sstring, sstring>& map_str);
    static bool is_me(sstring ip);

    static sstring build_extent_group_file_path(sstring main_dir, sstring extent_group_id);

    static sstring format(const std::unordered_set<gms::inet_address>&  set_address);
    static sstring format(const std::vector<gms::inet_address>&  vt_address);
    static sstring format(const std::vector<sstring>&  vt_str);
    static sstring format(const std::set<sstring>&  set_str);
    static sstring format(const std::map<sstring, sstring>&  map_str);

    static sstring join(const std::vector<sstring>& strings, sstring separator);

    static std::vector<sstring> filter(std::vector<sstring> values, sstring filter_value);

    static std::vector<sstring> deduplicate(std::vector<sstring> values);
    static bool exist(std::vector<sstring> values, sstring value);

    static uint64_t hash(sstring value, uint64_t range);
};

} //namespace hive
