// analytics.h - the box of numbers that the mapper, combiner and reducer share.
//
// Everything we keep is a count, a sum, a smallest or a biggest, so two boxes
// can always be added together. That is why the work can be split up at all.

#ifndef ANALYTICS_H
#define ANALYTICS_H

#include <map>
#include <string>
using namespace std;

// One line of the log file.
struct Record {
    long long timestamp;
    int server;
    int endpoint;
    int user;
    int status;
    long long time;      // response time, in millionths of a ms
    long long bytes;
};

// All the numbers the assignment asks for.
struct Stats {
    long long total = 0, success = 0, failed = 0;
    long long c2xx = 0, c3xx = 0, c4xx = 0, c5xx = 0;
    long long bytes = 0;
    long long time_sum = 0;
    long long time_min = -1;     // -1 means "nothing counted yet"
    long long time_max = -1;

    // We use map, not an array, because a mapper does not know in advance how
    // many servers or endpoints it will see.
    map<int, long long> server_count, server_time;
    map<int, long long> endpoint_count, endpoint_bytes;
    map<long long, long long> minute_count;
};

bool read_header(const string& line, int& k);      // the "N K S" first line
bool read_record(const string& line, Record& r);   // a normal log line

void add_stats(Stats& into, const Stats& other);   // add one box into another

void print_pairs(const Stats& s, int k);           // mapper/combiner output
bool read_pair(const string& line, Stats& s, int& k);   // read that back

#endif
