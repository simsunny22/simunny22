#include "hive/stream/session_info.hh"

namespace hive {

void session_info::update_progress(progress_info new_progress) {
    assert(peer == new_progress.peer);
    auto& current_files = new_progress.dir == progress_info::direction::IN
        ? receiving_files : sending_files;
    current_files[new_progress.file_name] = new_progress;
}

std::vector<progress_info> session_info::get_receiving_files() {
    std::vector<progress_info> ret;
    for (auto& x : receiving_files) {
        ret.push_back(x.second);
    }
    return ret;
}

std::vector<progress_info> session_info::get_sending_files() {
    std::vector<progress_info> ret;
    for (auto& x : sending_files) {
        ret.push_back(x.second);
    }
    return ret;
}

long session_info::get_total_size_in_progress(std::vector<progress_info> files) {
    long total = 0;
    for (auto& file : files) {
        total += file.current_bytes;
    }
    return total;
}

long session_info::get_total_files(std::vector<stream_summary>& summaries) {
    return summaries.size();
}

long session_info::get_total_sizes(std::vector<stream_summary>& summaries) {
    long total = 0;
    for (auto& summary : summaries)
        total += summary.total_size;
    return total;
}

long session_info::get_total_files_completed(std::vector<progress_info> files) {
    long size = 0;
    for (auto& x : files) {
        if (x.is_completed()) {
            size++;
        }
    }
    return size;
}

} // namespace hive 
