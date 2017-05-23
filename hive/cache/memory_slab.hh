#ifndef __HIVE_EXTENT_SLAB_ALLOCATOR__ 
#define __HIVE_EXTENT_SLAB_ALLOCATOR__

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp> 
#include <stdlib.h>
#include <assert.h>
#include <vector>
#include <algorithm>
#include "core/scollectd.hh"
#include "core/align.hh"
#include "core/memory.hh"
#include "log.hh"


namespace bi = boost::intrusive;

namespace hive {

class slab_item_base {
    bi::list_member_hook<> _lru_link;
    
    template<typename Item>
    friend class memory_slab_allocator;
};

template<typename Item>
class memory_slab_allocator {
private:
    bi::list<slab_item_base
           , bi::member_hook<slab_item_base, bi::list_member_hook<>
           , &slab_item_base::_lru_link> > _lru;

    std::vector<uintptr_t> _free_slabs;

    // Just for deconstroctor free memory
    // If item be locked, _lru and _free_slabs failed to track it
    std::vector<uintptr_t> _allocated_slabs;

    uint64_t _limit_size;

    // erase_func() is used to remove the item from the cache.
    std::function<void (Item& item_ref)> _erase_func;

private:
    template<typename... Args>
    inline Item* create_item(void *object, Args&&... args) {
        Item *new_item = new(object) Item(std::forward<Args>(args)...);
        _lru.push_front(reinterpret_cast<slab_item_base&>(*new_item));
        return new_item;
    }

    template<typename... Args>
    Item *create_from_free_slabs(Args&&... args) {
        assert(!_free_slabs.empty());
        auto idle_slab = _free_slabs.back();
        _free_slabs.pop_back();
        auto object = reinterpret_cast<void *>(idle_slab);
        return create_item(object, std::forward<Args>(args)...);
    }

    template<typename... Args>
    Item *create_from_lru(Args&&... args) {
        auto object = evict_lru_item();
        if (!object) {
            throw std::bad_alloc{};
        }
        return create_item(object, std::forward<Args>(args)...);
    }

    inline void *evict_lru_item() { 
        if (_lru.empty()) {
            return nullptr;
        }
        
        Item& victim = reinterpret_cast<Item&>(_lru.back());
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_item_base&>(victim)));
        // WARNING: You need to make sure that erase_func will not release victim back to slab.
        assert(_erase_func);
        _erase_func(victim);
        return reinterpret_cast<void*>(&victim);
    }
  
    void free_item(Item *item) {
        void *reclaim = item;
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_item_base&>(*item)));
        auto slab = reinterpret_cast<uintptr_t>(reclaim);
        _free_slabs.push_back(slab);
    }
    
    void touch_item(Item *item) {
        auto& item_ref = reinterpret_cast<slab_item_base&>(*item);
        _lru.erase(_lru.iterator_to(item_ref));
        _lru.push_front(item_ref);
    }
    
    void insert_item_into_lru(Item *item) {
        auto& item_ref = reinterpret_cast<slab_item_base&>(*item);
        _lru.push_front(item_ref);
    }

    void remove_item_from_lru(Item *item) {
        auto& item_ref = reinterpret_cast<slab_item_base&>(*item);
        _lru.erase(_lru.iterator_to(item_ref));
    }

    void initialize(uint64_t limit_size, uint64_t max_slab_size){
        assert(max_slab_size > 0);
        // allocate limit slabs
        uint64_t slab_count = limit_size / max_slab_size;
        _free_slabs.reserve(slab_count);
        _allocated_slabs.reserve(slab_count);
    
        // allocate slab 
        constexpr uint64_t alignment = std::alignment_of<Item>::value;
        for (uint64_t i = 0U; i < slab_count; i++) {
            void* object = aligned_alloc(alignment, max_slab_size);
            if (!object) {
                throw std::bad_alloc{};
            }
            memset(object, 0, max_slab_size);
            auto idle_slab = reinterpret_cast<uintptr_t>(object);
            _free_slabs.push_back(idle_slab);
            _allocated_slabs.push_back(idle_slab);
        }
    }

    void release(){
        // free slabs in _allocated_slabs
        for (auto slab : _allocated_slabs) {
            if (slab) {
                auto* object = reinterpret_cast<void *>(slab);
                ::free(object);
            }
        }
        _lru.clear();
        _free_slabs.clear();
        _free_slabs.shrink_to_fit();
        _allocated_slabs.clear();
        _allocated_slabs.shrink_to_fit();

    }
public:
    memory_slab_allocator(uint64_t limit_size
        , uint64_t max_slab_size
        , std::function<void (Item& item_ref)> erase_func)
        : _limit_size(limit_size)
        , _erase_func(std::move(erase_func)) {

        initialize(limit_size, max_slab_size);
    }

    ~memory_slab_allocator() {
        release();
    }

    bool empty() const {
        return _free_slabs.empty();
    }

    uint64_t free_slabs_count() const {
        return _free_slabs.size();
    }

    uint64_t limit() const {
        return _limit_size;
    }

    void lock_item(Item *item) {
        remove_item_from_lru(item);
    }

    void unlock_item(Item *item) {
        insert_item_into_lru(item);
    }

    void free(Item *item) {
        if(item){
            free_item(item);
        }
    }
    
    void touch(Item *item) {
        if(item){
            touch_item(item);
        }
    }

    template<typename... Args>
    Item *create(Args&&... args) {
        Item* item = nullptr;
        if (!empty()) {
            item = create_from_free_slabs(std::forward<Args>(args)...);
        }else if(_erase_func){
            item = create_from_lru(std::forward<Args>(args)...);
        }
        return item;
    }

}; //class memory_slab_allocator

} //namespace hive 

#endif
