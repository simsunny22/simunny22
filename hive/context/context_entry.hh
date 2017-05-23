#pragma once

namespace hive {

class context_entry{

public:
    context_entry(){}

    context_entry& operator=(context_entry& entry) noexcept{
        return *this;
    }

    context_entry& operator=(const context_entry& entry) noexcept {
        return *this;
    }

    context_entry& operator=(context_entry&& entry) noexcept {
        return *this;
    }
    
    //virtual context_entry* get_context_ptr() =  0;  
    virtual sstring get_entry_string(){
        return "";
    }
};

} //namespace hive
