#include <sstream>
#include <iostream>
#include <unistd.h>
#include <experimental/string_view>
#include <string>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include "core/align.hh"

#include "hive/cache/item_key.hh"
#include "hive/cache/memory_slab.hh"
#include "hive/cache/memory_cache.hh"

int main() {
  cache::memory_cache cache{3 * 1024 * 1024};
 
  using boost::lexical_cast;
  for (int i = 0; i < 7; i++) {
    cache::item_insertion_data insertion;
    std::string key_str = "foo" + lexical_cast<std::string>(i);
    cache::item_key key{key_str};
    insertion.key = key;
    insertion.data = "hello,world";
    insertion.version = 0U;
    cache.set(insertion);

    if (i < 4) {
      cache::item_key key{"foo0"};
      auto item = cache.get(key);
      std::cout << "[main] get item key:" << item->key().to_string() << " value:" << item->value().to_string() << std::endl;
    }
  }

  cache::item_key key{"foo0"};
  auto item = cache.get(key);
  if (item) {
     std::cout << "[main] get item key:" << item->key().to_string() << " value:" << item->value().to_string() << std::endl;
  } else {
    std::cout << "[main] item not found" << std::endl;
  }

  usleep(1 * 1000000);
  return 0;
}
