#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "utils/UUID.hh"
#include "core/seastar.hh"
#include "db/config.hh"
#include "hive/commitlog/replay_position.hh"


namespace hive {    

using namespace db;

using segment_id_type  = uint64_t;
using segment_pos_type = uint32_t;
using rp_map_type      = std::unordered_map<sstring, segment_pos_type>;
using rp_map_map_type  = std::unordered_map<int, rp_map_type>; //<cpu_id, rp_map_tpyes>
using drain_times_type = std::vector<int>; //<cpu_id, drain_times>

void         write_commitlog_rp_postion(sstring filename, rp_map_type& _rp_map);
rp_map_type  read_commitlog_rp_postion(sstring filename); 
rp_map_type  read_all_commitlog_rp_postion(sstring rp_log_dir);


class replay_position_log{

public:
    replay_position_log(sstring volume_id, db::config cfg);
    replay_position_log(replay_position_log&& rp_log) noexcept;
    ~replay_position_log();
    //int  _get_instance_num();
    void _write_rp_log(sstring volume_id, replay_position rp);

    void _set_drain_times(int id);
    void _reset_drain_times(int id);
    int  _get_drain_times(int id);

    rp_map_type  _set_rp_map_map(int id, sstring name, segment_pos_type segment_pos);
    void         _reset_rp_map_map(int id);
    rp_map_type  _get_rp_map_map(int id);


private:
    //rp_map_type _rp_map;
    sstring                  _volume_id;
    int                      _count;
    db::config               _cfg;
    static int               _instance_num;
    static drain_times_type  _drain_times;
    static rp_map_map_type   _cpu_rp_map;
};



}
