#include "system_time.hh"

namespace hive{

uint64_t system_time::get_seconds_diff(
        std::chrono::system_clock::time_point tp1
        , std::chrono::system_clock::time_point tp2){
    if(tp1 >= tp2){
        return std::chrono::duration_cast<std::chrono::seconds>(tp1 - tp2).count();
    }else{
        return std::chrono::duration_cast<std::chrono::seconds>(tp2 - tp1).count();
    }
}
// ====================================================
// public
// ====================================================
system_time::system_time(lw_shared_ptr<db::config> conf):_conf(conf){
    init_system_time();
}

system_time::~system_time(){
   
}

sstring system_time::get_5_min_time_slot(){ 
    return  _5_min_time_slot;
}

sstring system_time::get_10_second_time_slot(){
   return _10_second_time_slot;
}

std::chrono::system_clock::time_point 
system_time::get_now_tp(){
    return _now_tp;
}
// ====================================================
// private
// ====================================================
void system_time::init_system_time(){
   on_timer();
   auto time_slot = _conf->vega_system_time_sync_in_sec();
   _timer.set_callback(std::bind(&system_time::on_timer, this));
   _timer.arm(lowres_clock::now(), std::experimental::optional<lowres_clock::duration>{std::chrono::seconds(time_slot)});
}


void system_time::on_timer(){
    set_time_slot();
}

void system_time::set_time_slot(){
    _now_tp = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(_now_tp);
    //auto ptm = localtime(&tt);
    auto ptm = gmtime(&tt);
    auto year   = 1900 + ptm->tm_year;
    auto month  = 1    + ptm->tm_mon;
    auto day    = ptm->tm_mday;
    auto hour   = ptm->tm_hour;
    auto min    = ptm->tm_min;
    auto second = ptm->tm_sec; 

    _5_min_time_slot = padding_size(to_sstring(year))
         + padding_size(to_sstring(month))
         + padding_size(to_sstring(day))
         + padding_size(to_sstring(hour))
         + padding_size(to_sstring(min / 5 * 5));

   _10_second_time_slot = padding_size(to_sstring(year))
         + padding_size(to_sstring(month))
         + padding_size(to_sstring(day))
         + padding_size(to_sstring(hour))
         + padding_size(to_sstring(min))
         + padding_size(to_sstring(second / 10 * 10));

}

sstring system_time::padding_size(sstring raw){
    auto size = raw.size();
    switch(size){
      case 0:
        return to_sstring("00");
      case 1:
        return to_sstring("0" + raw);
      default:
         return raw;
    }
}

}//hive
