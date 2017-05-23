#include <fstream>
#include "hive/commitlog/replay_position_log.hh" 
#include "seastar/core/temporary_buffer.hh"
#include "seastar/core/file.hh"
#include "seastar/core/reactor.hh"
#include "disk-error-handler.hh"
#include "checked-file-impl.hh"
#include "log.hh"

#define BASE_DIR  "/opt/vega/data/hive/rp_log/rp_"
#define SUFFIX    ".log"
#define SEPARATOR "_"
#define MAX_SIZE  20

namespace hive {

static logging::logger   logger("replay_postion_log");

int               replay_position_log::_instance_num = 0;
drain_times_type  replay_position_log::_drain_times = std::vector<int>(16, 0); //16 cpu
rp_map_map_type   replay_position_log::_cpu_rp_map;

replay_position_log::replay_position_log(sstring volume_id, db::config cfg)
    :_volume_id(volume_id)
    ,_count(0)
    ,_cfg(cfg){
    _instance_num ++;
}

replay_position_log::replay_position_log(replay_position_log&& rp_log) noexcept
    :_volume_id(rp_log._volume_id)
    ,_count(rp_log._count)
    ,_cfg(std::move(rp_log._cfg)){
}

replay_position_log::~replay_position_log(){
    _instance_num--;
}


void replay_position_log::_set_drain_times(int id){
    if(id < (int)_drain_times.size()){  //id:current_core_id, _drain_times.size():total_core num
        _drain_times[id] = _drain_times[id] + 1;
    }else{
        int i = 0;
        while((int)_drain_times.size() + i < id){
            _drain_times.push_back(0);
        }
        _drain_times.push_back(1);
    }
}

void replay_position_log::_reset_drain_times(int id){
    if(id < (int)_drain_times.size()){
        _drain_times[id] = 0;
    }
}


int replay_position_log::_get_drain_times(int id){
    if(id < (int)_drain_times.size()){
        return _drain_times[id];
    }else{
        return 0;
    }

}


rp_map_type replay_position_log::_set_rp_map_map(int id, sstring commitlog_name, segment_pos_type pos){
    auto rp_map = _get_rp_map_map(id); //get one core rp map
    rp_map[commitlog_name] = pos;
    _cpu_rp_map[id] = rp_map;
    return rp_map;
}


void replay_position_log::_reset_rp_map_map(int id){
     rp_map_type rp_map;
    _cpu_rp_map[id] = rp_map;
}

rp_map_type replay_position_log::_get_rp_map_map(int id){
    auto it = _cpu_rp_map.find(id);
    if(it != _cpu_rp_map.end()){
        return it-> second;
    }else{
        rp_map_type rp_map;
        return  rp_map;
    }
}

//enter func
void replay_position_log::_write_rp_log(sstring commitlog_name, replay_position rp){
    auto id = engine().cpu_id();
    segment_pos_type  segment_pos = rp.pos;

    auto rp_map = _set_rp_map_map(id, commitlog_name, segment_pos);


    _set_drain_times(id);
    auto size = _get_drain_times(id);

    auto max_size = _cfg.replay_position_log_max_drain_times();// start write file util get one_core max times 
    if (size >= int(max_size)){
        sstring filename = _cfg.replay_position_log_directory() + "/rp_"+ to_sstring(engine().cpu_id()) + SUFFIX;
        write_commitlog_rp_postion(filename, rp_map);
        _reset_drain_times(id);
        _reset_rp_map_map(id);
    } 
}

void write_commitlog_rp_postion(sstring filename, rp_map_type& rp_map){
    auto rp_map_read = read_commitlog_rp_postion(filename);
    
    std::fstream file(filename,  std::ios::out);
    
    if(file){
       for(auto it = rp_map_read.begin(); it != rp_map_read.end(); it ++){
           auto commitlog_name  = it->first;
           auto segment_pos = it->second;
           
           rp_map.insert(std::make_pair<sstring, segment_pos_type>(std::move(commitlog_name), std::move(segment_pos)));
       }
        for(auto it  = rp_map.begin(); it != rp_map.end(); it++){
            auto  commitlog_name  = it->first;
            auto  segment_pos = it->second;
            file << commitlog_name  << " "<< segment_pos << std::endl;
         }
         file.close();

    }
} 

rp_map_type read_commitlog_rp_postion(sstring filename){
    rp_map_type rp_map;

    std::fstream file(filename, std::ios::in); 
    if(!file.is_open()){
        return rp_map;
    }
    while(!file.eof()){
        sstring  commitlog_file;
        segment_pos_type last_drain_pos;

        file >> commitlog_file >> last_drain_pos ;
        if(commitlog_file.empty()){
            continue;
        }
        rp_map.insert(std::make_pair<sstring, segment_pos_type>(std::move(commitlog_file), std::move(last_drain_pos)));
    }

    file.close();
    return rp_map;
}

rp_map_type read_all_commitlog_rp_postion(sstring rp_log_dir){
    logger.debug("[{}] start", __func__);
    rp_map_type rp_map;
    std::vector<sstring> need_unlink_files;

    // 1. read files
    for(unsigned i = 0; i < smp::count; i++){
        sstring filename = rp_log_dir + "/rp_"+ to_sstring(engine().cpu_id()) + SUFFIX;
        need_unlink_files.push_back(filename);
        auto rp_map_read = read_commitlog_rp_postion(filename);
        for(auto it  = rp_map_read.begin(); it != rp_map_read.end(); it++){
            auto  commitlog_file = it->first;
            auto  last_drain_pos = it->second;
            rp_map.insert(std::make_pair<sstring, segment_pos_type>(std::move(commitlog_file), std::move(last_drain_pos)));
        }
    }

    // 2. unlink files
    for(auto file : need_unlink_files ){
        ::unlink(file.c_str());
        logger.debug("[{}] unlink file:{}", __func__, file);
    }
  
    return rp_map;
}

} //namespace hive


