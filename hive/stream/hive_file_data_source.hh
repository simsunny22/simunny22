#ifndef __HIVE_FILE_DATA_SOURCE__
#define __HIVE_FILE_DATA_SOURCE__

#include "core/iostream.hh"
#include "core/fstream.hh"
#include "core/file.hh"

namespace hive {

class hive_file_data_source_impl : public data_source_impl {
    file _file;
    file_input_stream_options _options;
    uint64_t _pos;
    uint64_t _remain;
    circular_buffer<future<temporary_buffer<char>>> _read_buffers;
    unsigned _reads_in_progress = 0;
    std::experimental::optional<promise<>> _done;
public:
    hive_file_data_source_impl(file f, uint64_t offset, uint64_t len, file_input_stream_options options)
            : _file(std::move(f)), _options(options), _pos(offset), _remain(len) {
        // prevent wraparounds
        _remain = std::min(std::numeric_limits<uint64_t>::max() - _pos, _remain);
    }
    virtual future<temporary_buffer<char>> get() override {
        if (_read_buffers.empty()) {
            issue_read_aheads(1);
        }
        auto ret = std::move(_read_buffers.front());
        _read_buffers.pop_front();
        return ret;
    }
    virtual future<> close() {
        _done.emplace();
        if (!_reads_in_progress) {
            _done->set_value();
        }
        return _done->get_future().then([this] {
            for (auto&& c : _read_buffers) {
                c.ignore_ready_future();
            }
        });
    }
private:
    void issue_read_aheads(unsigned min_ra = 0) {
        if (_done) {
            return;
        }
        auto ra = std::max(min_ra, _options.read_ahead);
        while (_read_buffers.size() < ra) {
            if (!_remain) {
                if (_read_buffers.size() >= min_ra) {
                    return;
                }
                _read_buffers.push_back(make_ready_future<temporary_buffer<char>>());
                continue;
            }
            ++_reads_in_progress;
            // if _pos is not dma-aligned, we'll get a short read.  Account for that.
            // Also avoid reading beyond _remain.
            uint64_t align = _file.disk_read_dma_alignment();
            auto start = align_down(_pos, align);
            auto end = align_up(std::min(start + _options.buffer_size, _pos + _remain), align);
            auto len = end - start;
            _read_buffers.push_back(_file.dma_read_bulk<char>(start, len, _options.io_priority_class).then_wrapped(
                    [this, start, end, pos = _pos, remain = _remain] (future<temporary_buffer<char>> ret) {
                issue_read_aheads();
                --_reads_in_progress;
                if (_done && !_reads_in_progress) {
                    _done->set_value();
                }
                if ((pos == start && end <= pos + remain) || ret.failed()) {
                    // no games needed
                    return ret;
                } else {
                    // first or last buffer, need trimming
                    auto tmp = ret.get0();
                    auto real_end = start + tmp.size();
                    if (real_end <= pos) {
                        return make_ready_future<temporary_buffer<char>>();
                    }
                    if (real_end > pos + remain) {
                        tmp.trim(pos + remain - start);
                    }
                    if (start < pos) {
                        tmp.trim_front(pos - start);
                    }
                    return make_ready_future<temporary_buffer<char>>(std::move(tmp));
                }
            }));
            auto old_pos = _pos;
            _pos = end;
            _remain = std::max(_pos, old_pos + _remain) - _pos;
        };
    }
};

class hive_file_data_source : public data_source {
public:
    hive_file_data_source(file f, uint64_t offset, uint64_t len, file_input_stream_options options={})
        : data_source(std::make_unique<hive_file_data_source_impl>(std::move(f), offset, len, options)) {}
};

} //namespace hive

#endif
