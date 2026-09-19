// common.cpp - the shared counting and printing code.

#include "common.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

long long interval_of(long long timestamp)
{
    return timestamp / 60;      // the assignment says: timestamp / 60
}

bool parse_line(const std::string& line, Record& r)
{
    // sscanf is used instead of a C++ stringstream because stringstreams are
    // many times slower, and we read up to fifty million lines.
    int got = std::sscanf(line.c_str(), "%lld %d %d %d %d %lf %lld",
                          &r.timestamp,
                          &r.server_id,
                          &r.endpoint_id,
                          &r.user_id,
                          &r.status_code,
                          &r.response_time,
                          &r.bytes_sent);
    return (got == 7);
}

void find_ranges(const std::vector<Record>& recs,
                 int& max_server, int& max_endpoint,
                 long long& min_time, long long& max_time)
{
    // Start with "impossible" values so the first record replaces them.
    // If there are no records at all these stay as they are, and the caller
    // checks for that case.
    max_server   = -1;
    max_endpoint = -1;
    min_time     =  0;
    max_time     = -1;

    for (size_t i = 0; i < recs.size(); i++) {
        if (recs[i].server_id   > max_server)   max_server   = recs[i].server_id;
        if (recs[i].endpoint_id > max_endpoint) max_endpoint = recs[i].endpoint_id;

        if (i == 0) {
            min_time = recs[i].timestamp;
            max_time = recs[i].timestamp;
        } else {
            if (recs[i].timestamp < min_time) min_time = recs[i].timestamp;
            if (recs[i].timestamp > max_time) max_time = recs[i].timestamp;
        }
    }
}

void Stats::create(int num_servers, int num_endpoints,
                   long long first_interval_in, long long num_intervals)
{
    if (num_servers   < 1) num_servers   = 1;
    if (num_endpoints < 1) num_endpoints = 1;
    if (num_intervals < 1) num_intervals = 1;

    first_interval = first_interval_in;

    // assign() makes the vector the given size and fills it with zeros.
    // Unlike malloc in C, we never have to free these - the vectors clean
    // themselves up when the Stats object goes away.
    server_count.assign(num_servers, 0);
    server_time.assign(num_servers, 0.0);
    endpoint_count.assign(num_endpoints, 0);
    endpoint_bytes.assign(num_endpoints, 0);
    interval_count.assign(num_intervals, 0);
}

void Stats::count(const std::vector<Record>& recs)
{
    for (size_t i = 0; i < recs.size(); i++) {
        const Record& r = recs[i];

        total++;

        // The assignment says: a request is successful if status code < 400.
        if (r.status_code < 400) success++;
        else                     failed++;

        if      (r.status_code >= 200 && r.status_code <= 299) count_2xx++;
        else if (r.status_code >= 300 && r.status_code <= 399) count_3xx++;
        else if (r.status_code >= 400 && r.status_code <= 499) count_4xx++;
        else if (r.status_code >= 500 && r.status_code <= 599) count_5xx++;

        total_bytes += r.bytes_sent;

        // Add to the SUM, do not compute an average here.
        time_sum += r.response_time;
        if (r.response_time < time_min) time_min = r.response_time;
        if (r.response_time > time_max) time_max = r.response_time;

        // Count this request for its server.
        if (r.server_id >= 0 && r.server_id < (int)server_count.size()) {
            server_count[r.server_id]++;
            server_time[r.server_id] += r.response_time;
        }

        // Count this request for its endpoint.
        if (r.endpoint_id >= 0 && r.endpoint_id < (int)endpoint_count.size()) {
            endpoint_count[r.endpoint_id]++;
            endpoint_bytes[r.endpoint_id] += r.bytes_sent;
        }

        // Count this request for its 60-second interval.
        // We subtract first_interval so that our vector can start at 0.
        long long slot = interval_of(r.timestamp) - first_interval;
        if (slot >= 0 && slot < (long long)interval_count.size())
            interval_count[slot]++;
    }
}

// One row of a "top K" list, used only for sorting before we print.
struct TopRow {
    long long id;
    long long count;
    double    time_sum;   // used for servers
    long long bytes;      // used for endpoints
};

// std::sort calls this to decide which of two rows comes first.
// The assignment says: bigger count first; if the counts are equal,
// smaller id first.
static bool row_comes_first(const TopRow& a, const TopRow& b)
{
    if (a.count != b.count) return a.count > b.count;
    return a.id < b.id;
}

void Stats::print(int k) const
{
    double average  = 0.0;
    double show_min = 0.0;
    double show_max = 0.0;

    if (total > 0) {
        average  = time_sum / (double)total;   // divide only now
        show_min = time_min;
        show_max = time_max;
    }

    std::printf("TOTAL_REQUESTS %lld\n", total);
    std::printf("SUCCESSFUL_REQUESTS %lld\n", success);
    std::printf("FAILED_REQUESTS %lld\n", failed);
    std::printf("AVERAGE_RESPONSE_TIME %.6f\n", average);
    std::printf("MIN_RESPONSE_TIME %.6f\n", show_min);
    std::printf("MAX_RESPONSE_TIME %.6f\n", show_max);
    std::printf("TOTAL_BYTES %lld\n", total_bytes);
    std::printf("STATUS_2XX %lld\n", count_2xx);
    std::printf("STATUS_3XX %lld\n", count_3xx);
    std::printf("STATUS_4XX %lld\n", count_4xx);
    std::printf("STATUS_5XX %lld\n", count_5xx);

    // Busiest 60-second interval: just look for the largest count.
    // If two intervals tie, we keep the earlier one, because we only
    // replace the best when we find something strictly bigger.
    long long busy_interval = 0;
    long long busy_count    = 0;
    for (size_t i = 0; i < interval_count.size(); i++) {
        if (interval_count[i] > busy_count) {
            busy_count    = interval_count[i];
            busy_interval = first_interval + (long long)i;
        }
    }
    if (busy_count == 0) busy_interval = 0;   // no data at all
    std::printf("BUSIEST_INTERVAL %lld %lld\n", busy_interval, busy_count);

    // ---- top K servers ----
    std::printf("TOP_SERVERS\n");
    {
        std::vector<TopRow> rows;
        for (size_t i = 0; i < server_count.size(); i++) {
            if (server_count[i] > 0) {
                TopRow row;
                row.id       = (long long)i;
                row.count    = server_count[i];
                row.time_sum = server_time[i];
                row.bytes    = 0;
                rows.push_back(row);
            }
        }
        std::sort(rows.begin(), rows.end(), row_comes_first);

        int shown = (int)rows.size();
        if (shown > k) shown = k;          // k may be bigger than we have
        for (int i = 0; i < shown; i++) {
            double avg = rows[i].time_sum / (double)rows[i].count;
            std::printf("%lld %lld %.6f\n", rows[i].id, rows[i].count, avg);
        }
    }

    // ---- top K endpoints ----
    std::printf("TOP_ENDPOINTS\n");
    {
        std::vector<TopRow> rows;
        for (size_t i = 0; i < endpoint_count.size(); i++) {
            if (endpoint_count[i] > 0) {
                TopRow row;
                row.id       = (long long)i;
                row.count    = endpoint_count[i];
                row.time_sum = 0.0;
                row.bytes    = endpoint_bytes[i];
                rows.push_back(row);
            }
        }
        std::sort(rows.begin(), rows.end(), row_comes_first);

        int shown = (int)rows.size();
        if (shown > k) shown = k;
        for (int i = 0; i < shown; i++)
            std::printf("%lld %lld %lld\n", rows[i].id, rows[i].count, rows[i].bytes);
    }
}
