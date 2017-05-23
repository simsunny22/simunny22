#pragma once

#include <experimental/optional>

#include "frozen_mutation.hh"
#include "schema.hh"
#include "utils/data_output.hh"

namespace hive {

namespace stdx = std::experimental;
class hive_commitlog_entry {
    stdx::optional<column_mapping> _mapping;
    stdx::optional<frozen_mutation> _mutation_storage;
    const frozen_mutation& _mutation;
public:
    hive_commitlog_entry(stdx::optional<column_mapping> mapping, frozen_mutation&& mutation);
    hive_commitlog_entry(stdx::optional<column_mapping> mapping, const frozen_mutation& mutation);
    hive_commitlog_entry(hive_commitlog_entry&&);
    hive_commitlog_entry(const hive_commitlog_entry&) = delete;
    hive_commitlog_entry& operator=(hive_commitlog_entry&&);
    hive_commitlog_entry& operator=(const hive_commitlog_entry&) = delete;
    const stdx::optional<column_mapping>& mapping() const { return _mapping; }
    const frozen_mutation& mutation() const { return _mutation; }
};

class hive_commitlog_entry_writer {
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size;
private:
    void compute_size();
    hive_commitlog_entry get_entry() const;
public:
    hive_commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm)
        : _schema(s), _mutation(fm)
    {
        compute_size();
    }

    void set_with_schema(bool value) {
        _with_schema = value;
        compute_size();
    }
    bool with_schema() {
        return _with_schema;
    }
    schema_ptr schema() const {
        return _schema;
    }

    size_t size() const {
        return _size;
    }

    void write(data_output& out) const;
};

class hive_commitlog_entry_reader {
    hive_commitlog_entry _ce;
public:
    hive_commitlog_entry_reader(const temporary_buffer<char>& buffer);

    const stdx::optional<column_mapping>& get_column_mapping() const { return _ce.mapping(); }
    const frozen_mutation& mutation() const { return _ce.mutation(); }
};

} //namespace hive
