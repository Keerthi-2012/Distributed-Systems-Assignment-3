// mapper.cpp - THE MAP STEP. This is where every record is counted.
//
// Hadoop gives each mapper a piece of the input file on stdin, and reads
// "key<TAB>value" lines back from stdout. So a mapper is just a normal program,
// and you can test it with a pipe:
//
//     ./mapper < data/small.in | sort | ./combiner | sort | ./reducer
//
// This mapper counts its whole piece first and only then prints the totals. It
// does NOT print one line per record. Printing one line per record would push
// all 10 million records through the sort, which is the slow part of MapReduce.
// This way it prints one line per server, endpoint and minute it saw - a few
// thousand lines instead of millions. (That trick has a name: in-mapper
// combining.)
//
// The lines it prints:
//
//     K           the K from the file's first line (rows to show in the tops)
//     G           the overall counts for this piece
//     S:<id>      one server:   how many requests, total response time
//     E:<id>      one endpoint: how many requests, total bytes
//     I:<minute>  one minute:   how many requests
//
// Example - the first 3 records of tests/sample.in give:
//
//     K   2
//     G   3 2 1 2 0 1 0 4750 23500000 3250000 12500000
//     S:0 2 20250000
//     S:1 1 3250000
//     E:1 2 1750
//     E:2 1 3000
//     I:28333333 2
//     I:28333334 1

#include "../common/analytics.h"

#include <iostream>

// Which 60-second interval a timestamp belongs to. The timestamps are in
// seconds, so dividing by 60 gives the minute.
static long long minute_of(long long timestamp)
{
    return timestamp / 60;
}

// ---------------------------------------------------------------------------
// COUNT ONE RECORD. This is the actual work of the map step: every number the
// assignment asks for gets updated here, and nowhere else.
// ---------------------------------------------------------------------------
static void count(Stats& stats, const Record& r)
{
    stats.total++;

    // Successful means the request did not fail, i.e. the status is under 400.
    if (r.status_code < 400) stats.success++;
    else                     stats.failed++;

    // The same status also falls into one of the four groups.
    if      (r.status_code >= 200 && r.status_code <= 299) stats.count_2xx++;
    else if (r.status_code >= 300 && r.status_code <= 399) stats.count_3xx++;
    else if (r.status_code >= 400 && r.status_code <= 499) stats.count_4xx++;
    else if (r.status_code >= 500 && r.status_code <= 599) stats.count_5xx++;
    // A status outside all four ranges still counts in the totals above, but in
    // none of these four.

    stats.total_bytes += r.bytes_sent;

    // We keep the SUM of the response times, not the average, because sums can
    // be added together later and averages cannot. The reducer divides at the
    // very end.
    stats.total_time += r.response_time;

    // min_time and max_time start at -1, meaning "nothing counted yet", so the
    // first record always replaces them.
    if (stats.min_time == -1 || r.response_time < stats.min_time)
        stats.min_time = r.response_time;
    if (stats.max_time == -1 || r.response_time > stats.max_time)
        stats.max_time = r.response_time;

    // Per server: how many requests, and their total response time. Looking up
    // servers[id] creates the entry the first time we see that server.
    // A negative id is not a real server, so it is skipped here - it still
    // counts in the totals above.
    if (r.server_id >= 0) {
        ServerInfo& server = stats.servers[r.server_id];
        server.count++;
        server.total_time += r.response_time;
    }

    // Per endpoint: how many requests, and their total bytes.
    if (r.endpoint_id >= 0) {
        EndpointInfo& endpoint = stats.endpoints[r.endpoint_id];
        endpoint.count++;
        endpoint.total_bytes += r.bytes_sent;
    }

    // Per minute: just how many requests. The reducer picks the busiest one.
    stats.intervals[minute_of(r.timestamp)]++;
}

int main()
{
    Stats stats;
    int k = -1;             // -1 means this piece did not contain the header
    string line;

    // Read our piece of the file, line by line, and count it.
    while (getline(cin, line)) {
        if (line.empty()) continue;

        // The very first line of the file is "N K S", not a record. Only the
        // mapper that gets the start of the file sees it.
        long long n;
        int file_k, s;
        if (read_header(line, n, file_k, s)) {
            k = file_k;
            continue;
        }

        Record r;
        if (read_record(line, r)) count(stats, r);
    }

    // Print one line per server, endpoint and minute we saw.
    stats.write(k);
    return 0;
}
