#pragma once

#include "gms/inet_address.hh"
#include "hive/stream/stream_summary.hh"
#include "hive/stream/stream_session_state.hh"
#include "hive/stream/progress_info.hh"
#include <vector>
#include <map>

namespace hive {

/**
 * Stream session info.
 */
class session_info {
public:
    using inet_address = gms::inet_address;
    inet_address peer;
    /** Immutable collection of receiving summaries */
    std::vector<stream_summary> receiving_summaries;
    /** Immutable collection of sending summaries*/
    std::vector<stream_summary> sending_summaries;
    /** Current session state */
    stream_session_state state;

    std::map<sstring, progress_info> receiving_files;
    std::map<sstring, progress_info> sending_files;

    session_info() = default;
    session_info(inet_address peer_,
                 std::vector<stream_summary> receiving_summaries_,
                 std::vector<stream_summary> sending_summaries_,
                 stream_session_state state_)
        : peer(peer_)
        , receiving_summaries(std::move(receiving_summaries_))
        , sending_summaries(std::move(sending_summaries_))
        , state(state_) {
    }

    bool is_failed() const {
        return state == stream_session_state::FAILED;
    }

    /**
     * Update progress of receiving/sending file.
     *
     * @param newProgress new progress info
     */
    void update_progress(progress_info new_progress);

    std::vector<progress_info> get_receiving_files();

    std::vector<progress_info> get_sending_files();

    /**
     * @return total number of files already received.
     */
    long get_total_files_received() {
        return get_total_files_completed(get_receiving_files());
    }

    /**
     * @return total number of files already sent.
     */
    long get_total_files_sent() {
        return get_total_files_completed(get_sending_files());
    }

    /**
     * @return total size(in bytes) already received.
     */
    long get_total_size_received() {
        return get_total_size_in_progress(get_receiving_files());
    }

    /**
     * @return total size(in bytes) already sent.
     */
    long get_total_size_sent() {
        return get_total_size_in_progress(get_sending_files());
    }

    /**
     * @return total number of files to receive in the session
     */
    long get_total_files_to_receive() {
        return get_total_files(receiving_summaries);
    }

    /**
     * @return total number of files to send in the session
     */
    long get_total_files_to_send() {
        return get_total_files(sending_summaries);
    }

    /**
     * @return total size(in bytes) to receive in the session
     */
    long get_total_size_to_receive() {
        return get_total_sizes(receiving_summaries);
    }

    /**
     * @return total size(in bytes) to send in the session
     */
    long get_total_size_to_send() {
        return get_total_sizes(sending_summaries);
    }

private:
    long get_total_size_in_progress(std::vector<progress_info> files);

    long get_total_files(std::vector<stream_summary>& summaries);

    long get_total_sizes(std::vector<stream_summary>& summaries);

    long get_total_files_completed(std::vector<progress_info> files);
};

} // namespace hive 
