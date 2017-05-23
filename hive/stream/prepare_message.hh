#pragma once

#include "hive/stream/stream_request.hh"
#include "hive/stream/stream_summary.hh"

namespace hive {

class prepare_message {
public:
    /**
     * Streaming requests
     */
    std::vector<stream_request> requests;

    /**
     * Summaries of streaming out
     */
    std::vector<stream_summary> summaries;

    uint32_t dst_cpu_id;

    prepare_message() = default;
    prepare_message(std::vector<stream_request> reqs, std::vector<stream_summary> sums, uint32_t dst_cpu_id_ = -1)
      : requests(std::move(reqs))
      , summaries(std::move(sums))
      , dst_cpu_id(dst_cpu_id_) {
    }

};

} // namespace streaming
