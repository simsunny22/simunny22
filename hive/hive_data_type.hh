#pragma once

#include "seastar/core/sstring.hh"
#include "bytes.hh"
#include "hive/commitlog/replay_position.hh"

namespace hive{
class sync_commitlog_data_type{
public:
    sstring volume_id;
    sstring commitlog_id;
    sstring commitlog_file;
    uint64_t offset; 
    uint64_t size;
    bytes content;
    sync_commitlog_data_type(sstring  volume_id, sstring commitlog_id, sstring commitlog_file
        , uint64_t offset, uint64_t size, bytes content)
        :volume_id(volume_id), commitlog_id(commitlog_id)
        , commitlog_file(commitlog_file), offset(offset), size(size), content(content)
    {}

    sync_commitlog_data_type(){}

};

class discard_commitlog_data_type{
public:
    sstring volume_id;
    sstring commitlog_id;
    replay_position rp;
    discard_commitlog_data_type(sstring  volume_id, sstring commitlog_id, replay_position rp)
    :volume_id(volume_id), commitlog_id(commitlog_id), rp(std::move(rp)) 
    {}
    discard_commitlog_data_type(){}
};

class write_data_type{
public:
    lw_shared_ptr<const frozen_mutation> frozen_mutation_ptr = nullptr;
};

class abstract_data_type{
public:
   write_data_type write_data;
   sync_commitlog_data_type  sync_commitlog_data;
   discard_commitlog_data_type  discard_commitlog_data;
};


} //namespace hive
