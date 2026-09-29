// Mapper - reads its piece of the log file and prints one line per key it saw.
//
//   ./mapper < data/small.in | sort | ./combiner | sort | ./reducer
//
// It counts the whole piece first and only then prints, so it prints a few
// thousand lines instead of one per record. That is what keeps the sort cheap.

#include "analytics.h"
#include <iostream>

// Count one record into the box. This is the whole map step.
void count(Stats& s, const Record& r)
{
    s.total++;

    if (r.status < 400) s.success++;
    else                s.failed++;

    if      (r.status >= 200 && r.status <= 299) s.c2xx++;
    else if (r.status >= 300 && r.status <= 399) s.c3xx++;
    else if (r.status >= 400 && r.status <= 499) s.c4xx++;
    else if (r.status >= 500 && r.status <= 599) s.c5xx++;

    s.bytes    += r.bytes;
    s.time_sum += r.time;      // the sum, never an average - averages cannot be added

    if (s.time_min == -1 || r.time < s.time_min) s.time_min = r.time;
    if (s.time_max == -1 || r.time > s.time_max) s.time_max = r.time;

    if (r.server >= 0) {       // a negative id is not a real server
        s.server_count[r.server]++;
        s.server_time[r.server] += r.time;
    }
    if (r.endpoint >= 0) {
        s.endpoint_count[r.endpoint]++;
        s.endpoint_bytes[r.endpoint] += r.bytes;
    }

    s.minute_count[r.timestamp / 60]++;    // which 60-second interval it is in
}

int main()
{
    Stats s;
    int k = -1;                // -1 means this piece had no header line
    string line;

    while (getline(cin, line)) {
        if (line.empty()) continue;

        int file_k;
        if (read_header(line, file_k)) { k = file_k; continue; }

        Record r;
        if (read_record(line, r)) count(s, r);
    }

    print_pairs(s, k);
    return 0;
}
