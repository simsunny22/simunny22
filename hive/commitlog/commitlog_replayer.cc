#include <memory>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>

#include <core/future.hh>
#include <core/sharded.hh>

#include "hive/commitlog/hive_commitlog.hh"
#include "hive/commitlog/commitlog_replayer.hh"
#include "database.hh"
#include "sstables/sstables.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"
#include "log.hh"
#include "converting_mutation_partition_applier.hh"
#include "schema_registry.hh"
#include "hive/commitlog/hive_commitlog_entry.hh"
#include "hive/stream_service.hh"
#include "hive/journal/journal_service.hh"
#include "hive/volume_service.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"
#include "replay_position_log.hh"

namespace hive {

static logging::logger logger("commitlog_replayer");

/////////////////////////////////////////
// start commitlog_replayer::impl define
/////////////////////////////////////////
class hive::commitlog_replayer::impl {
    std::unordered_map<table_schema_version, column_mapping> _column_mappings;
public:
    impl();

    future<> init(sstring rp_log_dir);

    struct stats {
        uint64_t invalid_mutations = 0;
        uint64_t skipped_mutations = 0;
        uint64_t applied_mutations = 0;
        uint64_t corrupt_bytes = 0;

        stats& operator+=(const stats& s) {
            invalid_mutations += s.invalid_mutations;
            skipped_mutations += s.skipped_mutations;
            applied_mutations += s.applied_mutations;
            corrupt_bytes += s.corrupt_bytes;
            return *this;
        }
        stats operator+(const stats& s) const {
            stats tmp = *this;
            tmp += s;
            return tmp;
        }
    };

    future<> process(stats*, temporary_buffer<char> buf, replay_position rp);
    future<stats> replay(sstring file);
    
    //xiaowu add start
    rp_map_type _fp_map;
    //xiaowu add end

    typedef std::unordered_map<utils::UUID, replay_position> rp_map;
    typedef std::unordered_map<unsigned, rp_map> shard_rpm_map;
    typedef std::unordered_map<unsigned, replay_position> shard_rp_map;

    shard_rpm_map _rpm;
    shard_rp_map _min_pos;
}; //class hive::commitlog_replayer::impl

hive::commitlog_replayer::impl::impl(){}

//xiaowu add start
future<> hive::commitlog_replayer::impl::init(sstring rp_log_dir) {
    logger.debug("[{}] start", __func__);
    _fp_map = read_all_commitlog_rp_postion(rp_log_dir);
    return make_ready_future<>();
}
//xiaowu add end

future<hive::commitlog_replayer::impl::stats>
hive::commitlog_replayer::impl::replay(sstring file) {
    //xiaowu add start
    hive_commitlog::descriptor desc(file);
    position_type last_drain_pos = 0;
    last_drain_pos = _fp_map[desc.filename()]; 
    logger.info("[impl::replay] start, file:{}, last_drain_pos:{}", file, last_drain_pos);
    //xiaowu add end
    auto s = make_lw_shared<stats>();

    return hive::hive_commitlog::read_log_file(file,
            std::bind(&impl::process, this, s.get(), std::placeholders::_1, std::placeholders::_2)
            , last_drain_pos).then([](auto ret) {
        auto f = ret->done();
        
        return f.finally([ret=std::move(ret)] {});
    }).then_wrapped([s](future<> f) {
        try {
            f.get();
        } catch (hive_commitlog::segment_data_corruption_error& e) {
            s->corrupt_bytes += e.bytes();
        } catch (...) {
            throw;
        }
        return make_ready_future<stats>(*s);
    });
}

future<> hive::commitlog_replayer::impl::process(stats* s, temporary_buffer<char> buf, replay_position rp) {
    try {
        logger.info("[impl::process] start, rp:{}", rp);
        hive_commitlog_entry_reader cer(buf);
        auto& fm = cer.mutation();
        
        auto cm_it = _column_mappings.find(fm.schema_version());
        if (cm_it == _column_mappings.end()) {
            if (!cer.get_column_mapping()) {
                throw std::runtime_error(sprint("[impl::process] error unknown schema version {}", fm.schema_version()));
            }
            logger.debug("[impl::process] new schema version {} in entry pos:{}", fm.schema_version(), rp);
            cm_it = _column_mappings.emplace(fm.schema_version(), *cer.get_column_mapping()).first;
        }
        
        sstring volume_id = hive::hive_tools::parse_volume_id(fm);
        sstring extent_group_id = hive::hive_tools::parse_extent_group_id(fm);
        int64_t length = hive::hive_tools::parse_length(fm);

        logger.debug("[impl::process] volume_id:{}, extent_group_id:{}", volume_id, extent_group_id);
        //return make_exception_future<>(std::runtime_error("wztest........"));
        auto shard = hive::get_local_token_service().shard_of(extent_group_id);
        auto throttle_flag = make_lw_shared<bool>(false);
        return hive::get_token_service().invoke_on(shard, [length, volume_id, extent_group_id, throttle_flag]
                (auto& token_service){
            return token_service.throttle_memory(volume_id, length).then([volume_id, extent_group_id, throttle_flag](auto flag){
                //if(NORMAL != hive_service_status ){
                //    throw std::runtime_error("hive service is not normal"); 
                //}
                
                *throttle_flag = flag;
                logger.debug("[handle] get token done, volume_id:{}, extent_group_id", __func__, volume_id, extent_group_id);
            }); 
        }).then([this, volume_id, extent_group_id, rp, s](){
            auto shard = hive::get_local_volume_service().shard_of(volume_id);                              
            return hive::get_volume_service().invoke_on(shard, [volume_id] (auto& shard_volume_service){ 
                return shard_volume_service.bind_volume_driver(volume_id);
                });
        }).then([this, volume_id, extent_group_id, cer=std::move(cer), rp, s](auto driver_info)mutable{
            auto& local_journal_service = hive::get_local_journal_service();
            auto shard = local_journal_service.shard_of(extent_group_id); 
            return hive::get_journal_service().invoke_on(shard, [this, volume_id, extent_group_id
                    , cer=std::move(cer), rp, s](auto& shard_journal_service) mutable{
                auto primary_journal = shard_journal_service.get_primary_journal(volume_id);
                //auto& fm = cer.mutation();
                //primary_journal->apply(fm, hive::get_local_journal_service().get_schema(), rp);
                return do_with(std::move(cer), [primary_journal, volume_id](auto& cer){
                    auto& fm = cer.mutation();
                    return primary_journal->apply_mutate(fm);
                }).then([s, volume_id, extent_group_id, rp](){
                    logger.debug("[imp::process] replay end, volume_id:{}, extent_group_id:{}, at pos:{}"
                        , volume_id, extent_group_id, rp);
                    s->applied_mutations++;
                    return make_ready_future<>();
                }); 
            });
        }).then_wrapped([this, volume_id, extent_group_id, length, s, throttle_flag](auto f){
            try {   
                f.get();
                return make_ready_future<>();
            } catch (memory_insufficient_exception &e){
                s->invalid_mutations++;
                logger.error(e.what());
                return make_exception_future<>(std::runtime_error(e.what()));
            } catch (...) {
                if(*throttle_flag == true){
                    auto shard = hive::get_local_token_service().shard_of(extent_group_id);
                    hive::get_token_service().invoke_on(shard, [volume_id, length](auto& token_service){
                        token_service.unthrottle_memory(volume_id, length);
                    });
                }
                s->invalid_mutations++;
                std::ostringstream out;
                out << "[imp::process] error";
                out << ", exception:" << std::current_exception();
                auto error_info = out.str();
                logger.error(error_info.c_str());
                return make_exception_future<>(std::runtime_error(error_info));
            }
        });
    }  catch (...) {
        s->invalid_mutations++;
        std::ostringstream out;
        out << "[imp::process] error";
        out << ", exception:" << std::current_exception();
        auto error_info = out.str();
        logger.error(error_info.c_str());
        return make_exception_future<>(std::runtime_error(error_info));
    }
}



/////////////////////////////////////////
// start commitlog_replayer define
/////////////////////////////////////////

hive::commitlog_replayer::commitlog_replayer()
    : _impl( std::make_unique<impl>() )
{}



hive::commitlog_replayer::commitlog_replayer(commitlog_replayer&& r) noexcept
    : _impl(std::move(r._impl))
{}

hive::commitlog_replayer::~commitlog_replayer()
{}


future<hive::commitlog_replayer> hive::commitlog_replayer::create_replayer(sstring rp_log_dir) {
    return do_with(commitlog_replayer(), [rp_log_dir](auto&& replayer) {
        auto f = replayer._impl->init(rp_log_dir);
        return f.then([replayer=std::move(replayer)]() mutable {
            return make_ready_future<commitlog_replayer>(std::move(replayer));
        });
    });
}

future<> hive::commitlog_replayer::replay(std::vector<sstring> files) {
    logger.info("[{}] start, files:{}", __func__, join(",", files) );
    auto it = files.begin();
    auto end = files.end();
    impl::stats total_stats;
      
    return do_with(std::move(files), std::move(it), std::move(end), std::move(total_stats),
        [this](auto& files, auto& it, auto& end, auto& total_stats){

        auto stop_func = [&it, &end](){
            return it == end;
        };
        auto aciton_func = [this, &it, &total_stats](){
            return _impl->replay(*it).then_wrapped([&it, &total_stats](auto f){
                try{
                    auto stats = f.get0();
                    if (stats.corrupt_bytes != 0) {
                        logger.warn("[replay] warn Corrupted file: {}. {} bytes skipped.", *it, stats.corrupt_bytes);
                        //tododl::red only print warn log is ok???
                    }
                    logger.debug("[replay] replay of {} complete, {} replayed mutations ({} invalid, {} skipped)"
                        , *it
                        , stats.applied_mutations
                        , stats.invalid_mutations
                        , stats.skipped_mutations);              

                    total_stats += stats;
                    it++;
                    return make_ready_future<>();
                }catch(...){
                    logger.error("[replay] error recovering {}, exception:{}", std::current_exception());
                    throw;
                }

                
            });
        };
        return do_until(std::move(stop_func), std::move(aciton_func));
   });
}

future<> hive::commitlog_replayer::replay(sstring f) {
    return replay(std::vector<sstring>{ f });
}

} //namespace hive
