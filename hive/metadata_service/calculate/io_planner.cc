#include "io_planner.hh"
#include <math.h>

namespace hive{

std::vector<rw_split_item> io_planner::split_data(sstring volume_id, uint64_t offset, uint64_t length) {
    std::vector<rw_split_item> items;
    //offset is 1M(1024*1024) multiple
    if ((offset % extent_size) == 0){
        uint64_t count = std::ceil(length/double(extent_size));
        for(uint64_t i = 0; i< count; i++){
             rw_split_item item;
             item.extent_id = volume_id+"#"+to_sstring((offset/extent_size+i));
	           item.extent_offset_in_volume = (offset/extent_size+i) * extent_size;
             item.data_offset = i*extent_size;
             item.data_offset_in_extent = 0;

             uint64_t next_length = (offset+length)%extent_size;
             if (i == count-1 && next_length != 0){
                 item.length = next_length;
             }else{
                 item.length = extent_size;
             }
             items.push_back(item);
        }
    }else {
      // offset is not 1M(1024*1024) multiple
      uint64_t new_offset = (offset/extent_size + 1)*extent_size;
      if ((offset+length)<= new_offset) {
          rw_split_item item;
          item.extent_id = volume_id+"#"+to_sstring(offset/extent_size);
	        item.extent_offset_in_volume = (offset/extent_size)*extent_size;
          item.data_offset = 0;
          item.length = length;
          item.data_offset_in_extent = offset%extent_size;
          items.push_back(item);
      }else{
          rw_split_item item;
          item.extent_id = volume_id+"#"+to_sstring(offset/extent_size);
	        item.extent_offset_in_volume = (offset/extent_size)*extent_size;
          item.data_offset = 0;
          item.length = new_offset - offset;
          item.data_offset_in_extent = offset%extent_size;

          items.push_back(item);

          uint64_t new_length = length - (new_offset - offset);
          uint64_t count = std::ceil(new_length/double(extent_size));
          for(uint64_t i = 0; i< count; i++){
              rw_split_item next_item;
              next_item.extent_id = volume_id+"#"+to_sstring((new_offset/extent_size+i));
	            next_item.extent_offset_in_volume = (new_offset/extent_size + i)*extent_size;
              next_item.data_offset = i*extent_size  + (new_offset - offset);
              next_item.data_offset_in_extent = 0;

              uint64_t next_length = (new_offset+new_length)%extent_size;
              if (i == count-1 && next_length != 0){
                  next_item.length = next_length;
              }else {
                  next_item.length = extent_size;
              }
              items.push_back(next_item);
          }
      }
    }
    return items;
}

}//namespace hive
