#pragma once
#include "abstract_trail.hh"
#include "hive/hive_tools.hh"
#include "schema.hh"

namespace hive{

enum class access_trail_type{
    UNKNOWN_TYPE = 0,
    READ,
    WRITE,
    RANDOM_READ,
    RANDOM_WRITE,
    SEQUENCE_READ,
    SEQUENCE_WRITE,
};


using extent_group_key = std::pair<sstring, access_trail_type>;
using access_trail_map  = std::map<extent_group_key, trail_data>;

class access_trail_params: public abstract_trail_params{
public:
   sstring  disk_ids;
   sstring  extent_group_id;
   sstring  node_id;
   uint64_t size;
   sstring  options;
   access_trail_type type;

   access_trail_params(sstring disk_ids, sstring extent_group_id 
       , sstring node_id, uint64_t size, sstring options, access_trail_type type)
           : disk_ids(disk_ids)
           , extent_group_id(extent_group_id)
           , node_id(node_id)
           , size(size)
           , options(options)
           , type(type){}
};

class access_trail : public abstract_trail {
public:
    access_trail(db::config config);
    ~access_trail();

    virtual future<> create_trail();
    virtual future<> read_trail();
    virtual future<> collect(trail_data data);

    future<> trace(shared_ptr<abstract_trail_params> params);
private:
    db::config _config;
    schema_ptr _schema;
    access_trail_map  _access_trail_map;

    timer<lowres_clock> _timer;
    timer<lowres_clock> _print_timer;
    semaphore _semaphore;

    uint64_t _pending_access_trail_count = 0;
    uint64_t _writing_access_trail_count = 0;
    uint64_t _map_cache_count = 0;
private:
    void init_schema();
    void on_timer();
    void on_print_timer();

    access_trail_type get_access_trail_type(trail_data data);

    trail_data make_w_access_trail_data(
        sstring disk_id, 
        sstring extent_group_id, 
        sstring node_id,
        int64_t size,
        sstring options);

    trail_data make_r_access_trail_data(
        sstring disk_id, 
        sstring extent_group_id, 
        sstring node_id,
        int64_t size,
        sstring options);

    future<> do_trace(std::vector<trail_data>& access_trail_data_v);
};


} //namespace hive

