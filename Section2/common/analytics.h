// analytics.h - the pieces the mapper, combiner and reducer all three need.
//
// THE MAIN IDEA
//
// Everything we count is a total, a sum, a smallest or a biggest. All of those
// can be added up later. So each mapper can count its own part of the file, and
// the reducer just adds the parts together. We never store an average, because
// averages cannot be added: we store the sum and the count, and divide only
// when printing.
//
// WHAT IS HERE AND WHAT IS NOT
//
// Only the things that really are shared live here: the Stats box itself, how to
// add two of them together, and the text format they travel in. The logic that
// belongs to one program stays in that program:
//
//     Q1_mapreduce/mapper.cpp    how one record is counted  (the map step)
//     Q1_mapreduce/reducer.cpp   how the final answer is printed (the reduce step)

#ifndef ANALYTICS_H
#define ANALYTICS_H

#include <map>
#include <string>

// Written once, here, so the rest of the code can say `string` and `map`
// instead of `std::string` and `std::map`.
using namespace std;

// One line of the log file.
struct Record {
    long long timestamp;
    int server_id;
    int endpoint_id;
    int user_id;
    int status_code;
    long long response_time;   // in millionths of a ms (12.5 ms -> 12500000)
    long long bytes_sent;
};

// What we remember about one server.
struct ServerInfo {
    long long count;
    long long total_time;      // sum of response times, same unit as above
};

// What we remember about one endpoint.
struct EndpointInfo {
    long long count;
    long long total_bytes;
};

// All the numbers the assignment asks for. This is just a box of running totals:
// the mapper fills one in, and the reducer adds many of them together.
struct Stats {
    long long total;
    long long success;
    long long failed;
    long long count_2xx;
    long long count_3xx;
    long long count_4xx;
    long long count_5xx;
    long long total_bytes;

    long long total_time;      // sum of all response times
    long long min_time;        // -1 while nothing has been counted
    long long max_time;

    // We use map instead of a plain array because a mapper does not know in
    // advance how many servers or endpoints it will see. A map grows by itself.
    map<int, ServerInfo> servers;
    map<int, EndpointInfo> endpoints;
    map<long long, long long> intervals;   // minute number -> how many requests

    Stats();                        // starts everything at zero

    // Add another Stats into this one. This is the whole reason the job can be
    // split up at all, so it is shared by the combiner and the reducer.
    void add(const Stats& other);

    // Print as "key<TAB>value" lines - the format that travels from the mapper
    // to the combiner to the reducer. read_pair() below reads them back, so the
    // two must always agree. The mapper and the combiner both print this.
    void write(int k) const;
};

// Turn one line of text into a Record. Returns false if the line is not a
// record (the first line of the file, for example).
bool read_record(const string& line, Record& r);

// Read the "N K S" first line. Returns false if the line is not that.
bool read_header(const string& line, long long& n, int& k, int& s);

// Read one "key<TAB>value" line written by write() and add it into stats.
// If the line is the K line, k is set instead. Returns false if the line is not
// one of ours.
bool read_pair(const string& line, Stats& stats, int& k);

#endif
