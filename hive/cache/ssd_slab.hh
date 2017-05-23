#ifndef __HIVE_EXTENT_SSD_SLAB_ALLOCATOR__
#define __HIVE_EXTENT_SSD_SLAB_ALLOCATOR__

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp> 
#include <stdlib.h>
#include <assert.h>
#include <memory>
#include <vector>
#include <algorithm>

#include "log.hh"

namespace bi = boost::intrusive;
namespace hive {

class slab_ssd_item_base {
    bi::list_member_hook<> _lru_link;

    template<typename Item>
    friend class ssd_slab_allocator;
};

template<typename Item>
class ssd_slab_allocator {
private:
    bi::list<slab_ssd_item_base
           , bi::member_hook<slab_ssd_item_base, bi::list_member_hook<>
           , &slab_ssd_item_base::_lru_link>> _lru;

    std::vector<Item*> _free_slabs;
    std::vector<Item*> _allocated_slabs;

    uint64_t _limit_size;

    // erase_func() is used to remove the item from the ssd_cache.
    std::function<void (Item& item_ref)> _erase_func;
private:
    template<typename... Args>
    inline Item* create_item(void *object, off_t offset, Args&&... args) {
        Item *new_item = new(object) Item(offset, std::forward<Args>(args)...);
        _lru.push_front(reinterpret_cast<slab_ssd_item_base&>(*new_item));
        return new_item;
    }
  
    template<typename... Args>
    Item *create_from_free_slabs(Args&&... args) {
        assert(!_free_slabs.empty());
        auto idle_slab = _free_slabs.back();
        _free_slabs.pop_back();
        auto slab_offset = idle_slab->offset();
        auto object = reinterpret_cast<void *>(idle_slab);
        return create_item(object, slab_offset, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    Item *create_from_lru(Args&&... args) {
        auto slab = evict_lru_item();
        if (!slab) {
            throw std::bad_alloc{};
        }
        auto slab_offset = slab->offset();
        return create_item(reinterpret_cast<void*>(slab), slab_offset, std::forward<Args>(args)...);
    }
 
    inline Item* evict_lru_item() {
        if (_lru.empty()) {
            return nullptr;
        }
        Item& victim = reinterpret_cast<Item&>(_lru.back());
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_ssd_item_base&>(victim)));
        assert(_erase_func);
        _erase_func(victim);
        return &victim;
    }
     
    void free_item(Item *item) {
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_ssd_item_base&>(*item)));
        _free_slabs.push_back(item);
    }
    
    void touch_item(Item *item) {
        auto& item_ref = reinterpret_cast<slab_ssd_item_base&>(*item);
        _lru.erase(_lru.iterator_to(item_ref));
        _lru.push_front(item_ref);
    }
    
    void remove_item_from_lru(Item *item) {
        auto& item_ref = reinterpret_cast<slab_ssd_item_base&>(*item);
        _lru.erase(_lru.iterator_to(item_ref));
    }
    
    void insert_item_into_lru(Item *item) {
        auto& item_ref = reinterpret_cast<slab_ssd_item_base&>(*item);
        _lru.push_front(item_ref);
    }

    void initialize(uint64_t limit_size, uint64_t max_slab_size){
        assert(max_slab_size > 0);
        uint64_t slab_count = limit_size / max_slab_size;
        _free_slabs.reserve(slab_count);
        _allocated_slabs.reserve(slab_count);
    
        uint64_t off_set = 0U;
        for (uint64_t i = 0U; i < slab_count; i++) {
            Item* idle_slab = new Item{off_set}; 
            if (!idle_slab) {
                throw std::bad_alloc{};
            }
            _free_slabs.push_back(idle_slab);
            _allocated_slabs.push_back(idle_slab);
            off_set += max_slab_size;
        }
    }

    void release(){
        for(auto slab : _allocated_slabs){
            if(slab) {
                delete slab; 
            }
        } 
        _lru.clear();
        _free_slabs.clear();
        _free_slabs.shrink_to_fit();
        _allocated_slabs.clear();
        _allocated_slabs.shrink_to_fit();
    }
public:
    ssd_slab_allocator(uint64_t limit_size, uint64_t max_slab_size, std::function<void (Item& item_ref)> erase_func)
        : _limit_size(limit_size)
        , _erase_func(std::move(erase_func))
    {
        initialize(limit_size, max_slab_size);
    }
    
    ~ssd_slab_allocator() {
        release();
    }
    
    bool empty() const {
        return _free_slabs.empty();
    }
    
    size_t free_slabs_count() const {
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
        if (item) {
            free_item(item);
        }
    }
    
    void touch(Item *item) {
        if (item) {
            touch_item(item);
        }
    }
 
    template<typename... Args>
    Item *create(Args&&... args) {
        Item* item = nullptr;
        if (!empty()) {
            item = create_from_free_slabs(std::forward<Args>(args)...);
        } else if (_erase_func) {
            item = create_from_lru(std::forward<Args>(args)...);
        }
        return item;
    }

}; //class ssd_slab_allocator

} //namespace hive

#endif
