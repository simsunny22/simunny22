#pragma once
#include "core/timer.hh"
#include "core/reactor.hh"
#include "db/config.hh"
#include "log.hh"


namespace hive{

class system_time{
public:
   static uint64_t get_seconds_diff(
       std::chrono::system_clock::time_point tp1
       ,std::chrono::system_clock::time_point tp2
   );
   system_time(lw_shared_ptr<db::config> conf);
   ~system_time();

   sstring get_5_min_time_slot();
   sstring get_10_second_time_slot();
   std::chrono::system_clock::time_point 
   get_now_tp();
private:
   lw_shared_ptr<db::config> _conf;
   timer<lowres_clock> _timer;

   sstring _5_min_time_slot;
   sstring _10_second_time_slot;

   std::chrono::system_clock::time_point _now_tp;
   //tm* _local_time = nullptr;

private:
   void init_system_time();
   void on_timer();
   void set_time_slot();
   sstring padding_size(sstring raw);
};

}//hive

