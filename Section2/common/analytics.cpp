// analytics.cpp - the shared part: adding two Stats together, and the text
// format they travel in.

#include "analytics.h"

#include <cstdio>

bool read_record(const string& line, Record& r)
{
    double time_ms = 0;
    int got = sscanf(line.c_str(), "%lld %d %d %d %d %lf %lld",
                     &r.timestamp, &r.server_id, &r.endpoint_id, &r.user_id,
                     &r.status_code, &time_ms, &r.bytes_sent);
    if (got != 7) return false;

    // The response time is kept as a whole number instead of a decimal.
    // Decimals lose a tiny amount each time you add them, and the order we add
    // them in changes from run to run, so the last digits could come out
    // different. Whole numbers always add up to exactly the same answer.
    //
    // The unit is a millionth of a millisecond, because the output prints 6
    // decimal places and we must not lose any of them: 12.5 ms is stored as
    // 12500000, and 0.000001 ms as 1.
    r.response_time = (long long)(time_ms * 1000000.0 + 0.5);
    return true;
}

bool read_header(const string& line, long long& n, int& k, int& s)
{
    long long extra;
    // A header line has 3 numbers, a record has 7. So if a 4th number can be
    // read, this is a record and not the header.
    return sscanf(line.c_str(), "%lld %d %d %lld", &n, &k, &s, &extra) == 3;
}

Stats::Stats()
{
    total = 0;
    success = 0;
    failed = 0;
    count_2xx = 0;
    count_3xx = 0;
    count_4xx = 0;
    count_5xx = 0;
    total_bytes = 0;
    total_time = 0;
    min_time = -1;      // -1 means "nothing counted yet"
    max_time = -1;
}

void Stats::add(const Stats& other)
{
    total += other.total;
    success += other.success;
    failed += other.failed;
    count_2xx += other.count_2xx;
    count_3xx += other.count_3xx;
    count_4xx += other.count_4xx;
    count_5xx += other.count_5xx;
    total_bytes += other.total_bytes;
    total_time += other.total_time;

    // -1 means the other side counted nothing, so it has no smallest or biggest.
    if (other.min_time != -1 && (min_time == -1 || other.min_time < min_time))
        min_time = other.min_time;
    if (other.max_time != -1 && (max_time == -1 || other.max_time > max_time))
        max_time = other.max_time;

    for (const auto& it : other.servers) {
        servers[it.first].count += it.second.count;
        servers[it.first].total_time += it.second.total_time;
    }
    for (const auto& it : other.endpoints) {
        endpoints[it.first].count += it.second.count;
        endpoints[it.first].total_bytes += it.second.total_bytes;
    }
    for (const auto& it : other.intervals) {
        intervals[it.first] += it.second;
    }
}

// The "G" line: every single-number total, in one fixed order. write() prints
// them and read_pair() reads them back, so the two must stay in step.
#define G_FORMAT "%lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld"

void Stats::write(int k) const
{
    // k is -1 when this piece of the file did not contain the header line.
    if (k >= 0) printf("K\t%d\n", k);

    if (total > 0) {
        printf("G\t" G_FORMAT "\n",
               total, success, failed,
               count_2xx, count_3xx, count_4xx, count_5xx,
               total_bytes, total_time, min_time, max_time);
    }

    for (const auto& it : servers)
        printf("S:%d\t%lld %lld\n", it.first, it.second.count, it.second.total_time);
    for (const auto& it : endpoints)
        printf("E:%d\t%lld %lld\n", it.first, it.second.count, it.second.total_bytes);
    for (const auto& it : intervals)
        printf("I:%lld\t%lld\n", it.first, it.second);
}

// Read one line written by write(), like "S:3\t12 4500", and add it in.
bool read_pair(const string& line, Stats& stats, int& k)
{
    // Split at the tab: the key is on the left, the numbers on the right.
    size_t tab = line.find('\t');
    if (tab == string::npos) return false;
    string key = line.substr(0, tab);
    const char* value = line.c_str() + tab + 1;

    if (key == "K") return sscanf(value, "%d", &k) == 1;

    if (key == "G") {
        Stats part;
        long long mn = -1, mx = -1;
        int got = sscanf(value, G_FORMAT,
                         &part.total, &part.success, &part.failed,
                         &part.count_2xx, &part.count_3xx, &part.count_4xx,
                         &part.count_5xx, &part.total_bytes, &part.total_time,
                         &mn, &mx);
        if (got != 11) return false;
        part.min_time = mn;
        part.max_time = mx;
        stats.add(part);        // add() already knows how to combine two Stats
        return true;
    }

    // The remaining keys look like "S:3", "E:12" or "I:28333333": a letter, a
    // colon, then the id. Anything shorter is not one of ours - checking the
    // length first is what stops us reading past the end of the key.
    if (key.size() < 3 || key[1] != ':') return false;

    long long id = 0;
    if (sscanf(key.c_str() + 2, "%lld", &id) != 1) return false;

    long long a = 0, b = 0;
    if (key[0] == 'S' && sscanf(value, "%lld %lld", &a, &b) == 2) {
        stats.servers[(int)id].count += a;
        stats.servers[(int)id].total_time += b;
        return true;
    }
    if (key[0] == 'E' && sscanf(value, "%lld %lld", &a, &b) == 2) {
        stats.endpoints[(int)id].count += a;
        stats.endpoints[(int)id].total_bytes += b;
        return true;
    }
    if (key[0] == 'I' && sscanf(value, "%lld", &a) == 1) {
        stats.intervals[id] += a;
        return true;
    }
    return false;
}
