
#include "hive/commitlog/hive_commitlog_entry.hh"
#include "hive/extent_datum.hh"
#include "hive/extent_revision.hh"
#include "hive/extent_revision_set.hh"
#include "hive/hive_data_type.hh"
#include "hive/message_data_type.hh"
#include "hive/test_message.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/stream_request.hh"
#include "hive/stream/stream_summary.hh"
#include "hive/stream/prepare_message.hh"
#include "hive/stream/migrate_chunk.hh"

#include "hive/commitlog/hive_commitlog_entry.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/hive.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/hive.dist.impl.hh"

namespace hive {

hive_commitlog_entry::hive_commitlog_entry(stdx::optional<column_mapping> mapping, frozen_mutation&& mutation)
    : _mapping(std::move(mapping))
      , _mutation_storage(std::move(mutation))
      , _mutation(*_mutation_storage)
{ }

hive_commitlog_entry::hive_commitlog_entry(stdx::optional<column_mapping> mapping, const frozen_mutation& mutation)
    : _mapping(std::move(mapping))
      , _mutation(mutation)
{ }

hive_commitlog_entry::hive_commitlog_entry(hive_commitlog_entry&& ce)
    : _mapping(std::move(ce._mapping))
    , _mutation_storage(std::move(ce._mutation_storage))
    , _mutation(_mutation_storage ? *_mutation_storage : ce._mutation)
{
}

hive_commitlog_entry& hive_commitlog_entry::operator=(hive_commitlog_entry&& ce)
{
    if (this != &ce) {
        this->~hive_commitlog_entry();
        new (this) hive_commitlog_entry(std::move(ce));
    }
    return *this;
}

hive_commitlog_entry hive_commitlog_entry_writer::get_entry() const {
    if (_with_schema) {
        return hive_commitlog_entry(_schema->get_column_mapping(), _mutation);
    } else {
        return hive_commitlog_entry({}, _mutation);
    }
}

void hive_commitlog_entry_writer::compute_size() {
    _size = ser::get_sizeof(get_entry());
}

void hive_commitlog_entry_writer::write(data_output& out) const {
    seastar::simple_output_stream str(out.reserve(size()));
    ser::serialize(str, get_entry());
}

hive_commitlog_entry_reader::hive_commitlog_entry_reader(const temporary_buffer<char>& buffer)
    : _ce([&] {
    seastar::simple_input_stream in(buffer.get(), buffer.size());
    return ser::deserialize(in, boost::type<hive_commitlog_entry>());
}())
{
}

} //namespace hive
