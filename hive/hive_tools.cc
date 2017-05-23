#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"
#include "hive/journal/journal_service.hh"
#include <regex>
#include <iostream>
#include <fstream>
#include <cryptopp/md5.h>
#include <seastar/core/sstring.hh> 
#include <sys/time.h>
#include <chrono>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include "disk-error-handler.hh"
#include "checked-file-impl.hh"


namespace hive{

int32_t hive_tools::str_to_int32(sstring str){
    return std::atoi(str.c_str());
}

uint32_t hive_tools::str_to_uint32(sstring str){
    return std::strtoul(str.c_str(), NULL, 0);
}

int64_t hive_tools::str_to_int64(sstring str){
    return std::atoll(str.c_str());
}

uint64_t hive_tools::str_to_uint64(sstring str){
    return std::strtoull(str.c_str(), NULL, 0);
}

int64_t hive_tools::get_current_time(){
    struct timeval dw_time;
    gettimeofday(&dw_time, NULL);
    int64_t cur_time = dw_time.tv_sec*1000000 + dw_time.tv_usec;
    return cur_time;
}

int64_t hive_tools::get_current_time_in_ms(){
    struct timeval dw_time;
    gettimeofday(&dw_time, NULL);
    int64_t cur_time = dw_time.tv_sec*1000 + dw_time.tv_usec/1000;
    return cur_time;
}

sstring hive_tools::parse_extent_group_id(const mutation& m){
    auto schema = m.schema();
    auto& partition = m.partition();
    sstring extent_group_id;
    for (auto& clustered_row: partition.clustered_rows()) {
        auto& row = clustered_row.row();
        if(!row.deleted_at()){
            auto& cells = row.cells();
            auto column_id = schema->get_column_definition("extent_group_id")->id;
            auto extent_group_id_tmp = cells.cell_at(column_id).as_atomic_cell().value();
            extent_group_id = value_cast<sstring>(utf8_type->deserialize(extent_group_id_tmp));
        }
        break;
    }
    return extent_group_id;
};

sstring hive_tools::parse_extent_group_id(const frozen_mutation& m){
    schema_ptr schema = hive::get_local_journal_service().get_schema();
    mutation mutation = m.unfreeze(schema);
    return parse_extent_group_id(mutation);
};

sstring hive_tools::parse_volume_id(const mutation& m){
    auto schema = m.schema();
    auto& partition = m.partition();
    sstring volume_id;
    for (auto& clustered_row: partition.clustered_rows()) {
        auto& row = clustered_row.row();
        if(!row.deleted_at()){
            auto& cells = row.cells();
            auto column_id = schema->get_column_definition("volume_id")->id;
            auto volume_id_tmp = cells.cell_at(column_id).as_atomic_cell().value();
            volume_id = value_cast<sstring>(utf8_type->deserialize(volume_id_tmp));
        }
        break;
    }
    return volume_id;
};

sstring hive_tools::parse_volume_id(const frozen_mutation& m){
    schema_ptr schema = hive::get_local_journal_service().get_schema();
    mutation mutation = m.unfreeze(schema);
    return parse_volume_id(mutation);
};

int64_t hive_tools::parse_length(const mutation& m){
    auto schema = m.schema();
    auto& partition = m.partition();
    int64_t length;
    for (auto& clustered_row: partition.clustered_rows()) {
        auto& row = clustered_row.row();
        if(!row.deleted_at()){
            auto& cells = row.cells();
            auto column_id = schema->get_column_definition("length")->id;
            auto length_tmp = cells.cell_at(column_id).as_atomic_cell().value();
            length = value_cast<int64_t>(long_type->deserialize(length_tmp));
            column_id = schema->get_column_definition("data")->id;
            auto content_length = cells.cell_at(column_id).as_atomic_cell().value().size();
            assert(length == int64_t(content_length));
        }
        break;
    }
    return length;
};

int64_t hive_tools::parse_length(const frozen_mutation& m){
    schema_ptr schema = hive::get_local_journal_service().get_schema();
    mutation mutation = m.unfreeze(schema);
    return parse_length(mutation);
};

bytes hive_tools::calculate_digest(const unsigned char* data_buf, int64_t data_size) {
    CryptoPP::Weak::MD5 hash;
    bytes digest(bytes::initialized_later(), hive_config::extent_header_digest_size);
    std::memset(digest.begin(), 0, hive_config::extent_header_digest_size);
    hash.CalculateDigest(reinterpret_cast<unsigned char*>(digest.begin())
                       , reinterpret_cast<const unsigned char*>(data_buf)
                       , data_size);
        
    return std::move(digest);
}

sstring hive_tools::calculate_md5(bytes& data) {
    bytes datum_digest = hive_tools::calculate_digest(reinterpret_cast<const unsigned char*>(data.begin())
                                                     ,data.size());
    return to_hex(datum_digest);
}

//utils::UUID hive_tools::generate_commitlog_id(sstring volume_id) {
//    sstring commitlog_id_str = volume_id + to_sstring(engine().cpu_id());
//    utils::UUID commitlog_id = utils::UUID_gen::get_name_UUID(
//        reinterpret_cast<const unsigned char *>(commitlog_id_str.c_str()), commitlog_id_str.length());
//    return commitlog_id;
//}

sstring hive_tools::generate_commitlog_id(sstring volume_id) {
    return volume_id + "_" + to_sstring<sstring>(engine().cpu_id());
}

sstring hive_tools::generate_journal_segment_id(sstring volume_id){
    std::chrono::system_clock::time_point time_point = std::chrono::system_clock::now();
    uint64_t l_time =  time_point.time_since_epoch().count();
    return volume_id + "_" + to_sstring(engine().cpu_id()) + "_" + to_sstring(l_time);
}

sstring hive_tools::digest_to_md5(std::array<uint8_t, 16> digest) {
    assert(digest.size()==16);
    bytes digest_tmp = bytes(reinterpret_cast<const int8_t*>(digest.data()), digest.size());
    return to_hex(digest_tmp);
}

parsed_commitlog_name hive_tools::parse_commitlog_file_name(sstring commitlog_name) {
    //tododl:yellow move this define to a common define???
    const std::string SEPARATOR("-");
    const std::string FILENAME_PREFIX("CommitLog" + SEPARATOR);
    const std::string FILENAME_EXTENSION(".log");

    std::regex rx("(?:.*/)?" + FILENAME_PREFIX
                 +"([a-zA-Z0-9_]+[_][0-9]+)" + SEPARATOR
                 + "((\\d+)(" + SEPARATOR
                 + "\\d+)?)" + FILENAME_EXTENSION);

    std::string sfilename = commitlog_name; 
    std::smatch m;
    if (!std::regex_match(sfilename, m, rx)) {
        throw std::domain_error("Cannot parse the volume_id of the file: " + commitlog_name);
    }
    if (m[4].length() == 0) {
        throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
    }

    sstring  commitlog_id = m[1].str();
    size_t   pos          = commitlog_id.find_last_of('_');
    sstring  volume_id    = commitlog_id.substr(0, pos);
    uint32_t cpu_id       = std::stoul(commitlog_id.substr(pos+1));

    uint32_t version    = std::stoul(m[3].str());
    uint64_t segment_id = std::stoull(m[4].str().substr(1));
    return std::move(parsed_commitlog_name{commitlog_id, volume_id, cpu_id, version, segment_id});
}

future<std::vector<sstring>> hive_tools::list_files(sstring dirname) {
    struct helper {
        sstring _dirname;
        file _file;
        subscription<directory_entry> _list;
        std::vector<sstring> _result;

        helper(helper&&) = default;
        helper(sstring n, file && f)
                : _dirname(std::move(n))
                , _file(std::move(f))
                , _list(_file.list_directory(std::bind(&helper::process
                                                     , this
                                                     , std::placeholders::_1))) 
        {
        }

        future<> process(directory_entry de) {
            auto entry_type = [this](const directory_entry & de) {
                if (!de.type && !de.name.empty()) {
                    return engine().file_type(_dirname + "/" + de.name);
                }
                return make_ready_future<std::experimental::optional<directory_entry_type>>(de.type);
            };
            return entry_type(de).then([this, de](std::experimental::optional<directory_entry_type> type) {
                if (type == directory_entry_type::regular && de.name[0] != '.') {
                    try {
                        sstring commitlog_file_path = _dirname + "/" + de.name;
                        _result.emplace_back(commitlog_file_path);
                    } catch (std::domain_error& e) {
                        throw;
                    }
                }
                return make_ready_future<>();
            });
        }

        future<> done() {
            return _list.done();
        }
    };
    return open_checked_directory(general_disk_error_handler, dirname).then([dirname](file dir) {
        auto h = make_lw_shared<helper>(std::move(dirname), std::move(dir));
        return h->done().then([h]() {
            return make_ready_future<std::vector<sstring>>(std::move(h->_result));
        }).finally([h] {});
    });
}

std::vector<sstring> hive_tools::split_to_vector(const sstring& text, const char* separator) {
    if (text == "") {
        return std::vector<sstring>();
    }
    std::vector<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}

std::unordered_set<sstring> hive_tools::split_to_unordered_set(const sstring& text, const char* separator) {
    if (text == "") {
        return std::unordered_set<sstring>();
    }
    std::unordered_set<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}

std::set<sstring> hive_tools::split_to_set(const sstring& text, const char* separator) {
    if (text == "") {
        return std::set<sstring>();
    }
    std::set<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}

std::unordered_map<sstring, sstring> 
hive_tools::upper_key(const std::unordered_map<sstring, sstring>& map_str){
    std::unordered_map<sstring, sstring> map_out;
    for(auto& pair : map_str){
        sstring upper_key = boost::to_upper_copy(pair.first);
        map_out.insert(std::make_pair(upper_key, pair.second));
    }
    return map_out;
}

std::unordered_map<sstring, sstring> 
hive_tools::lower_key(const std::unordered_map<sstring, sstring>& map_str){
    std::unordered_map<sstring, sstring> map_out;
    for(auto& pair : map_str){
        sstring upper_key = boost::to_lower_copy(pair.first);
        map_out.insert(std::make_pair(upper_key, pair.second));
    }
    return map_out;
}

bool hive_tools::is_me(sstring ip) {
    return gms::inet_address(ip) == utils::fb_utilities::get_broadcast_address();
}


sstring hive_tools::build_extent_group_file_path(sstring main_dir, sstring extent_group_id){
    size_t length = extent_group_id.length();
    assert(length >= 2);
    sstring sub_dir_name = extent_group_id.substr(length-2, 2);
    sstring seperator = hive_config::file_seperator;
    sstring file_path = main_dir + seperator + sub_dir_name + seperator + extent_group_id;
    return file_path;
}

sstring hive_tools::format(const std::unordered_set<gms::inet_address>&  set_address){
    if(set_address.size() <=0){
        return "";  
    }

    sstring build_str;
    for(auto& address : set_address){
        build_str += address.to_sstring() + ",";
    }
    return build_str.substr(0, build_str.length()-1); 
}

sstring hive_tools::format(const std::vector<gms::inet_address>&  vt_address){
    if(vt_address.size() <=0){
        return "";  
    }

    sstring build_str;
    for(auto& address : vt_address){
        build_str += address.to_sstring() + ",";
    }
    return build_str.substr(0, build_str.length()-1); 
}

sstring hive_tools::format(const std::vector<sstring>&  vt_str){
    if(vt_str.size() <=0){
        return "";  
    }

    sstring build_str;
    for(auto& str : vt_str){
        build_str += str + ",";
    }
    return build_str.substr(0, build_str.length()-1); 
}

sstring hive_tools::format(const std::set<sstring>&  set_str){
    if(set_str.size() <=0){
        return "";  
    }

    sstring build_str;
    for(auto& str : set_str){
        build_str += str + ",";
    }
    return build_str.substr(0, build_str.length()-1); 
}

sstring hive_tools::format(const std::map<sstring, sstring>&  map_str){
    if(map_str.size() <=0){
        return "";  
    }

    sstring build_str;
    for(auto& str : map_str){
        build_str += str.first + ":" + str.second + ",";
    }
    return build_str.substr(0, build_str.length()-1); 
}

sstring hive_tools::join(const std::vector<sstring>&  vt_str, sstring separator){
    if(vt_str.size() <=0){
        return "";  
    }

    sstring build_str;
    for(auto& str : vt_str){
        build_str += str + separator;
    }

    return build_str.substr(0, build_str.length()-1); 
}

uint64_t hive_tools::random(uint64_t range){
      return (rand()*rand()) % range + 1;
}

std::vector<sstring> hive_tools::filter(std::vector<sstring> values, sstring filter_value) {
    std::vector<sstring> results;
    for(auto& v : values){
        if(filter_value != v) {
            results.push_back(v);
        }
    }
    return results;
}

bool hive_tools::exist(std::vector<sstring> values, sstring value) {
    for(auto& v : values){
        if(v == value) {
           return true;
        }
    }
    return false;
}

std::vector<sstring> hive_tools::deduplicate(std::vector<sstring> values) {
    std::vector<sstring> results;
    for(auto& v : values){
        if(!exist(results, v)) {
            results.push_back(v);
        }
    }
    return results;
}

uint64_t hive_tools::hash(sstring value, uint64_t range){
  std::hash<std::string> h;
  uint64_t hash = h(value);
  return hash%range;
}

}//namespace hive
