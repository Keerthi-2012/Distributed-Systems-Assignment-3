// common.h
//
// Shared code used by BOTH programs:
//   log_seq.cpp  - the normal version, runs on 1 processor
//   log_mpi.cpp  - the MPI version, runs on many processors
//
// Both programs use the same Stats class, so they always compute the answers
// in exactly the same way.

#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>

// One line of the log file.
struct Record {
    long long timestamp;
    int       server_id;
    int       endpoint_id;
    int       user_id;
    int       status_code;
    double    response_time;
    long long bytes_sent;
};

// All the numbers the assignment asks us to compute.
//
// IMPORTANT IDEA: we store SUMS here, never averages.
// You cannot add two averages together to get the correct overall average,
// but you CAN add two sums. So we keep the sum and the count, and we divide
// only at the very end when printing. This is what lets the MPI version add
// up the results from all the processors correctly.
struct Stats {
    // simple counters
    long long total       = 0;
    long long success     = 0;
    long long failed      = 0;
    long long count_2xx   = 0;
    long long count_3xx   = 0;
    long long count_4xx   = 0;
    long long count_5xx   = 0;
    long long total_bytes = 0;

    // response time: we keep the SUM (not the average), plus min and max.
    // A processor with no records keeps min very high and max very low,
    // so it can never affect the combined answer.
    double time_sum = 0.0;
    double time_min =  1e300;
    double time_max = -1e300;

    // one slot per server: how many requests, and the sum of response times
    std::vector<long long> server_count;
    std::vector<double>    server_time;

    // one slot per endpoint: how many requests, and the sum of bytes
    std::vector<long long> endpoint_count;
    std::vector<long long> endpoint_bytes;

    // one slot per 60-second interval: how many requests happened in it.
    // first_interval tells us which interval slot 0 means, so that a
    // timestamp like 1700000000 does not need a gigantic array.
    long long              first_interval = 0;
    std::vector<long long> interval_count;

    // Makes the vectors the right size, all filled with zeros.
    void create(int num_servers, int num_endpoints,
                long long first_interval, long long num_intervals);

    // Goes through the records one by one and adds them in.
    // This is the actual counting work.
    void count(const std::vector<Record>& recs);

    // Prints the answers in the exact format the assignment asks for.
    void print(int k) const;
};

// Which 60-second interval does this timestamp belong to?
long long interval_of(long long timestamp);

// Turns one line of text into a Record. Returns true if it worked.
// Both programs use this, so they read data the same way.
bool parse_line(const std::string& line, Record& r);

// Looks through the records to find the largest server id, the largest
// endpoint id, and the smallest and largest timestamp. We need these before
// we can decide how big the vectors have to be.
void find_ranges(const std::vector<Record>& recs,
                 int& max_server, int& max_endpoint,
                 long long& min_time, long long& max_time);

#endif
