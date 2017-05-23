#include "hive/trail/access_trail.hh"
#include "hive/trail/trail_service.hh"
#include "hive/system_service/system_service.hh"
#include "seastar/core/reactor.hh"

#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "schema_builder.hh"
#include "schema_registry.hh"
#include "gc_clock.hh"
#include "log.hh"


namespace hive{

static logging::logger logger("access_trail");

static atomic_cell make_atomic_cell(bytes_view value,  gc_clock::duration ttl) { 
    if(ttl.count() == 0){
       return atomic_cell::make_live(api::new_timestamp(), value);
    }
    return atomic_cell::make_live(api::new_timestamp(), value, ttl);
}


static sstring get_access_id(sstring disk_id){
   auto& system_service = hive::get_local_system_service();
   auto timestamp = system_service.get_system_time_ptr()->get_5_min_time_slot();
   return disk_id + "#" + timestamp;
}

static sstring get_timestamp(){
  auto& system_service = hive::get_local_system_service();
  return system_service.get_system_time_ptr()->get_10_second_time_slot();
}


// ===================================================
// public func
// ===================================================
access_trail::access_trail(db::config config):_config(config)
                                                 ,_semaphore(10){
    init_schema();

    auto periodic = _config.periodic_print_vega_log_stats_in_s();
    if( periodic > 0){
        _print_timer.set_callback(std::bind(&access_trail::on_print_timer, this));
        _print_timer.arm(lowres_clock::now(), std::experimental::optional<lowres_clock::duration>{
                        std::chrono::seconds(periodic)});
    }

    auto flush_interval = _config.vega_log_flush_to_db_in_s();
    _timer.set_callback(std::bind(&access_trail::on_timer, this));
    _timer.arm(lowres_clock::now(), std::experimental::optional<lowres_clock::duration>{
                        std::chrono::seconds(flush_interval)});
}

access_trail::~access_trail(){
}


future<> access_trail::create_trail(){
    logger.debug("[{}] start", __func__);
    try {
        auto& local_migration = service::get_local_migration_manager();
        return local_migration.announce_new_column_family(_schema, false); 
    }catch(const exceptions::already_exists_exception& ex){
        logger.warn("[{}] warn, {}", __func__, ex.what());  
    }catch(...){
        throw; 
    }
    return make_ready_future<>();
}

future<> access_trail::read_trail(){
    return make_ready_future<>();
}

//future<> access_trail::trace(sstring disk_ids, sstring extent_group_id, uint64_t size, sstring options, access_trail_type type){
future<> access_trail::trace(shared_ptr<abstract_trail_params> abstract_params){
    if(_config.vega_log_enable()){
         auto  access_params = dynamic_pointer_cast<access_trail_params>(abstract_params);
         trail_data access_trail_data;
         auto disk_id_v = hive_tools::split_to_vector(access_params->disk_ids, ":");
         for(auto disk_id: disk_id_v){
              switch (access_params->type){
                case access_trail_type::READ:
                  access_trail_data = make_r_access_trail_data(disk_id
                                                             , access_params->extent_group_id
                                                             , access_params->node_id
                                                             , access_params->size
                                                             , access_params->options);
                  break;
                case access_trail_type::WRITE:
                  access_trail_data = make_w_access_trail_data(disk_id
                                                             , access_params->extent_group_id
                                                             , access_params->node_id
                                                             , access_params->size
                                                             , access_params->options);
                  break;
                default:
                  return make_ready_future<>();
              }
         }
         collect(access_trail_data);
    }

    return make_ready_future<>();
}

future<> access_trail::collect(trail_data access_trail_data){
   auto extent_group_id  = access_trail_data["extent_group_id"];
   auto access_tail_type = get_access_trail_type(access_trail_data);
   if (access_tail_type == access_trail_type::UNKNOWN_TYPE){
   }else{
         auto key = std::make_pair(extent_group_id, access_tail_type);
         auto it  =  _access_trail_map.find(key);
         if(it != _access_trail_map.end()){
             //key is already exist, so do nothing ...
         }else{
             //key is new, so insert
             _access_trail_map.insert(std::make_pair(key, access_trail_data));
             _pending_access_trail_count++;
         }
   }
   return make_ready_future<>();
        
}
// ===================================================
// private func
// ===================================================
void access_trail::init_schema() {
    logger.debug("[{}] start", __func__);

    sstring version_str = "access_trail";
    utils::UUID schema_version = utils::UUID_gen::get_name_UUID(
        reinterpret_cast<const unsigned char *>(version_str.c_str()), version_str.length() );

    schema_builder builder(make_lw_shared(schema(
        {schema_version}, 
        trail_service::keyspace_name, 
        "access_trail",
        // partition key
        {{"access_id", utf8_type}},
        // clustering key
        { 
            {"timestamp", utf8_type},
            {"extent_group_id", utf8_type },
        },
        // regular columns
        {   
            {"node_id", utf8_type},
            {"random_read", long_type},
            {"random_write", long_type},
            {"sequence_read", long_type},
            {"sequence_write", long_type},
            {"tier", utf8_type},
            {"options", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "extent group access log"
    )));
    builder.with_version(schema_version);
    builder.set_compaction_strategy(sstables::compaction_strategy_type::date_tiered);
    
    uint64_t time_to_live = _config.access_map_log_time_to_live_in_s();
    logger.debug("[{}] time_to_live:{}", __func__, time_to_live);
    builder.set_default_time_to_live(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::seconds(time_to_live)));

    _schema = builder.build(schema_builder::compact_storage::no);
}

void access_trail::on_print_timer(){
    logger.error("access_trail::on_print_timer func, pending_access_log_count:{} , writing_access_log_count:{}, map_cache_count:{}"
                , _pending_access_trail_count
                , _writing_access_trail_count
                , _map_cache_count);
}

void access_trail::on_timer(){
    //logger.error("[{}] start, pending_access_log_count:{}, writing_access_log_count:{}, map_cache_count:{}"
    //    , __func__
    //    , _pending_access_trail_count
    //    , _writing_access_trail_count
    //    , _map_cache_count);
#if 0
    _semaphore.wait(1).then([this](){
        if(_access_trail_map.size() > 0){
            _map_cache_count++;
            return do_with(std::move(_access_trail_map), [this](auto& access_trail_map)mutable{
                std::vector<trail_data> access_trail_data_v;
                for(auto& pair : access_trail_map){
                    auto access_trail_data = pair.second;
                    access_trail_data_v.push_back(access_trail_data);
                }
                _pending_access_trail_count -= access_trail_data_v.size(); 
                  return this->do_trace(access_trail_data_v);
              }).finally([this](){
                    _map_cache_count--;
              });
          }else{
          }
          return make_ready_future<>();
      }).finally([this](){
          _semaphore.signal(1);
      });
#endif
    _semaphore.wait(1).then([this](){
        if(_access_trail_map.size() > 0){
            _map_cache_count++;
            auto access_trail_map = std::move(_access_trail_map);
            std::vector<trail_data> access_trail_data_v;
            for(auto& pair : access_trail_map){
                auto access_trail_data = pair.second;
                access_trail_data_v.push_back(access_trail_data);
            }
            
            _pending_access_trail_count -= access_trail_data_v.size(); 
       
            return do_with(std::move(access_trail_data_v), [this](auto& access_trail_data_v)mutable{
                return this->do_trace(access_trail_data_v);
            }).finally([this](){
                _map_cache_count--;
            }); 
        }else {
            return make_ready_future<>();
        }

    }).finally([this](){
        _semaphore.signal(1);
    });
}


future<> access_trail::do_trace(std::vector<trail_data>& access_trail_data_v){
    auto write_count = access_trail_data_v.size();
    _writing_access_trail_count += write_count;

    std::vector<mutation> mutations;
    for(auto& access_trail_data : access_trail_data_v){
        sstring keyspace   = access_trail_data["keyspace"];
        sstring table_name = access_trail_data["table_name"];
        logger.debug("[{}] start, ks:{}, cf:{}, shard:{}", __func__, keyspace, table_name, engine().cpu_id());

        sstring access_id       = access_trail_data["access_id"];       //text
        sstring timestamp       = access_trail_data["timestamp"];       //text
        sstring extent_group_id = access_trail_data["extent_group_id"]; //text
        sstring node_id         = access_trail_data["node_id"];         //text
        sstring random_read     = access_trail_data["random_read"];     //bigint
        sstring random_write    = access_trail_data["random_write"];    //bigint
        sstring seqence_read    = access_trail_data["sequence_read"];   //bigint
        sstring seqence_write   = access_trail_data["sequence_write"];  //bigint
        sstring tier            = access_trail_data["tier"];            //tier
        sstring options         = access_trail_data["options"];         //text
        
        database& db = service::get_local_storage_proxy().get_db().local();
        auto& cf = db.find_column_family(keyspace, table_name);
        const schema_ptr& s = cf.schema();
        auto pk = partition_key::from_exploded(*s,  {to_bytes(access_id)});         //row key
        auto ck = clustering_key::from_exploded(*s, {to_bytes(timestamp), to_bytes(extent_group_id)});//clustering key

        const column_definition& node_id_def        = *s->get_column_definition("node_id");
        const column_definition& random_read_def    = *s->get_column_definition("random_read");
        const column_definition& random_write_def   = *s->get_column_definition("random_write");
        const column_definition& seqence_read_def   = *s->get_column_definition("sequence_read");
        const column_definition& seqence_write_def  = *s->get_column_definition("sequence_write");
        const column_definition& tier_def           = *s->get_column_definition("tier");
        const column_definition& options_def = *s->get_column_definition("options");

        auto ttl = s->default_time_to_live();
        logger.debug("[{}] ttl:{}", __func__, ttl.count());

        mutation m(pk, s);
        m.set_clustered_cell(ck, node_id_def,        make_atomic_cell(to_bytes(node_id), ttl));
        m.set_clustered_cell(ck, random_read_def,    make_atomic_cell(long_type->from_string(random_read), ttl));
        m.set_clustered_cell(ck, random_write_def,   make_atomic_cell(long_type->from_string(random_write), ttl));
        m.set_clustered_cell(ck, seqence_read_def,   make_atomic_cell(long_type->from_string(seqence_read), ttl));
        m.set_clustered_cell(ck, seqence_write_def,  make_atomic_cell(long_type->from_string(seqence_write), ttl));
        m.set_clustered_cell(ck, tier_def,           make_atomic_cell(to_bytes(tier), ttl));
        m.set_clustered_cell(ck, options_def,        make_atomic_cell(to_bytes(options), ttl));
        mutations.emplace_back(m);
    }

    auto& local_storage_proxy = service::get_local_storage_proxy();
    return local_storage_proxy.mutate(std::move(mutations), db::consistency_level::VEGA_LOG, nullptr).then([this, write_count]() mutable{
        this->_writing_access_trail_count -= write_count;
        return make_ready_future<>();
    });
}

trail_data access_trail::make_w_access_trail_data(sstring disk_id
                                                , sstring extent_group_id
                                                , sstring node_id
                                                , int64_t size
                                                , sstring options){
    trail_data data;
    data["keyspace"] = trail_service::keyspace_name;
    data["table_name"] = trail_service::get_trail_name(vega_trail_type::ACCESS_TRAIL);
    data["access_id"] = get_access_id(disk_id);
    data["timestamp"] = get_timestamp();
    data["extent_group_id"] = extent_group_id;
    data["node_id"] = node_id;
    data["random_read"] = "0";
    data["sequence_read"] = "0";
    data["tier"] = "ssd";   
    data["options"] = options;
    if(size >= 1024*1024){
        data["random_write"] = "0";
        data["sequence_write"] = "1";
    }else{
        data["random_write"] = "1";
        data["sequence_write"] = "0";
    }   
    return data;
}

trail_data access_trail::make_r_access_trail_data(sstring disk_id
                                                , sstring extent_group_id
                                                , sstring node_id
                                                , int64_t size
                                                , sstring options){
    trail_data data;
    data["keyspace"] = trail_service::keyspace_name;
    data["table_name"] = trail_service::get_trail_name(vega_trail_type::ACCESS_TRAIL);
    data["access_id"] = get_access_id(disk_id);    
    data["timestamp"] = get_timestamp();
    data["extent_group_id"] = extent_group_id;
    data["node_id"] = node_id;
    data["random_write"] = "0";
    data["sequence_write"] = "0";
    data["tier"] = "ssd";   
    data["options"] = options;
    if(size >= 1024*1024){
        data["random_read"] = "0";
        data["sequence_read"] = "1";
    }else{
        data["random_read"] = "1";
        data["sequence_read"] = "0";
    }   
    return data;
}

access_trail_type access_trail::get_access_trail_type(trail_data access_trail_data){
    sstring random_read     = access_trail_data["random_read"];     //int
    sstring random_write    = access_trail_data["random_write"];    //int
    sstring seqence_read    = access_trail_data["sequence_read"];    //int
    sstring seqence_write   = access_trail_data["sequence_write"];   //int
    if (random_read == "1") {
        return access_trail_type::RANDOM_READ;
    }else if(random_write == "1"){
        return access_trail_type::RANDOM_WRITE;
    }else if(seqence_read == "1"){
        return access_trail_type::SEQUENCE_READ;
    }else if( seqence_write == "1"){
        return access_trail_type::SEQUENCE_WRITE;
    }else{
        return access_trail_type::UNKNOWN_TYPE;
    }
    
}

}//namespace hive

