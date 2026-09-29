// analytics.cpp - reading log lines, adding two boxes together, and the
// "key<TAB>value" text that travels from the mapper to the reducer.

#include "analytics.h"
#include <cstdio>

// The first line of the file is "N K S". A record line has 7 numbers, so if a
// 4th number can be read it is a record, not the header.
bool read_header(const string& line, int& k)
{
    long long n, extra;
    int s;
    return sscanf(line.c_str(), "%lld %d %d %lld", &n, &k, &s, &extra) == 3;
}

bool read_record(const string& line, Record& r)
{
    double ms = 0;
    int got = sscanf(line.c_str(), "%lld %d %d %d %d %lf %lld",
                     &r.timestamp, &r.server, &r.endpoint, &r.user,
                     &r.status, &ms, &r.bytes);
    if (got != 7) return false;

    // Keep the response time as a whole number of millionths of a ms.
    // Decimals lose a little every time you add them, and mappers finish in a
    // different order each run, so the last digits could change. Whole numbers
    // always give exactly the same answer.
    r.time = (long long)(ms * 1000000.0 + 0.5);
    return true;
}

void add_stats(Stats& into, const Stats& other)
{
    into.total    += other.total;
    into.success  += other.success;
    into.failed   += other.failed;
    into.c2xx     += other.c2xx;
    into.c3xx     += other.c3xx;
    into.c4xx     += other.c4xx;
    into.c5xx     += other.c5xx;
    into.bytes    += other.bytes;
    into.time_sum += other.time_sum;

    // -1 means the other box counted nothing, so it has no smallest or biggest.
    if (other.time_min != -1 && (into.time_min == -1 || other.time_min < into.time_min))
        into.time_min = other.time_min;
    if (other.time_max != -1 && (into.time_max == -1 || other.time_max > into.time_max))
        into.time_max = other.time_max;

    for (auto& it : other.server_count)    into.server_count[it.first]    += it.second;
    for (auto& it : other.server_time)     into.server_time[it.first]     += it.second;
    for (auto& it : other.endpoint_count)  into.endpoint_count[it.first]  += it.second;
    for (auto& it : other.endpoint_bytes)  into.endpoint_bytes[it.first]  += it.second;
    for (auto& it : other.minute_count)    into.minute_count[it.first]    += it.second;
}

// The lines the mapper and the combiner print:
//
//   K           2
//   G           total success failed 2xx 3xx 4xx 5xx bytes time_sum min max
//   S:<id>      how many requests, total response time
//   E:<id>      how many requests, total bytes
//   I:<minute>  how many requests
void print_pairs(const Stats& s, int k)
{
    if (k >= 0) printf("K\t%d\n", k);

    if (s.total > 0)
        printf("G\t%lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld\n",
               s.total, s.success, s.failed, s.c2xx, s.c3xx, s.c4xx, s.c5xx,
               s.bytes, s.time_sum, s.time_min, s.time_max);

    for (auto& it : s.server_count)
        printf("S:%d\t%lld %lld\n", it.first, it.second,
               s.server_time.at(it.first));
    for (auto& it : s.endpoint_count)
        printf("E:%d\t%lld %lld\n", it.first, it.second,
               s.endpoint_bytes.at(it.first));
    for (auto& it : s.minute_count)
        printf("I:%lld\t%lld\n", it.first, it.second);
}

// Read one of those lines back and add it into s.
bool read_pair(const string& line, Stats& s, int& k)
{
    size_t tab = line.find('\t');
    if (tab == string::npos) return false;
    string key = line.substr(0, tab);
    const char* value = line.c_str() + tab + 1;

    if (key == "K") return sscanf(value, "%d", &k) == 1;

    if (key == "G") {
        Stats part;
        int got = sscanf(value, "%lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld",
                         &part.total, &part.success, &part.failed,
                         &part.c2xx, &part.c3xx, &part.c4xx, &part.c5xx,
                         &part.bytes, &part.time_sum, &part.time_min, &part.time_max);
        if (got != 11) return false;
        add_stats(s, part);
        return true;
    }

    // The rest look like "S:3", "E:12" or "I:28333333".
    if (key.size() < 3 || key[1] != ':') return false;
    long long id = 0;
    if (sscanf(key.c_str() + 2, "%lld", &id) != 1) return false;

    long long a = 0, b = 0;
    if (key[0] == 'S' && sscanf(value, "%lld %lld", &a, &b) == 2) {
        s.server_count[(int)id] += a;
        s.server_time[(int)id]  += b;
        return true;
    }
    if (key[0] == 'E' && sscanf(value, "%lld %lld", &a, &b) == 2) {
        s.endpoint_count[(int)id] += a;
        s.endpoint_bytes[(int)id] += b;
        return true;
    }
    if (key[0] == 'I' && sscanf(value, "%lld", &a) == 1) {
        s.minute_count[id] += a;
        return true;
    }
    return false;
}
