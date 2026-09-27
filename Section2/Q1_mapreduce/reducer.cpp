// reducer.cpp - THE REDUCE STEP. This is where the final answer is worked out.
//
// Hadoop sorts every line the mappers printed and feeds them to this program on
// stdin. Whatever it prints on stdout is the final answer.
//
//     ./mapper < data/small.in | sort | ./combiner | sort | ./reducer
//
// Adding the mappers' lines together is easy, because everything is a total, a
// sum, a smallest or a biggest:
//
//     two mappers          this reducer
//     G   3 ... 4750       total   3 + 2 = 5
//     G   2 ... 1080       bytes   4750 + 1080 = 5830
//     S:0 2 20250000       server 0   2 + 1 = 3 requests
//     S:0 1 1000000                   20250000 + 1000000 = 21250000
//     I:28333334 1         minute 28333334   1 + 2 = 3   <- the busiest
//     I:28333334 2
//
// WHY ONLY ONE REDUCER
//
// Most of the answers could be split over many reducers. The two "top K" lists
// could not: to know the top 10 servers you need the counts of ALL servers. A
// server that is 11th on every reducer can still be 1st overall. So one reducer
// sees everything. That is cheap here, because the mappers already shrank the
// data to a few thousand lines.
//
// K comes from the file's first line, passed along by the mapper that saw it.
// If it never arrives we use 10. --k N overrides it (the tests use this).

#include "../common/analytics.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

// Response times are stored as whole numbers of millionths of a millisecond
// (see read_record). This turns them back into milliseconds for printing.
static double to_ms(long long value)
{
    return value / 1000000.0;
}

// One row of a "top K" list. The maps are copied into a list of these so they
// can be sorted - a map is sorted by id, and we need it sorted by count.
struct TopRow {
    long long id;
    long long count;
    long long value;    // total response time for servers, total bytes for endpoints
};

// The assignment's sorting rule: more requests first, and if two have the same
// number of requests, the smaller id first.
static bool is_before(const TopRow& a, const TopRow& b)
{
    if (a.count != b.count) return a.count > b.count;
    return a.id < b.id;
}

// Sort the rows and keep only the best k. Both top lists need exactly this.
static vector<TopRow> best_k(vector<TopRow> rows, int k)
{
    sort(rows.begin(), rows.end(), is_before);
    if (k < 0) k = 0;
    if ((int)rows.size() > k) rows.resize(k);
    return rows;
}

// ---------------------------------------------------------------------------
// PRINT THE ANSWER. Everything below is worked out from the added-up totals.
// ---------------------------------------------------------------------------
static void print_report(const Stats& stats, int k)
{
    printf("TOTAL_REQUESTS %lld\n", stats.total);
    printf("SUCCESSFUL_REQUESTS %lld\n", stats.success);
    printf("FAILED_REQUESTS %lld\n", stats.failed);

    // Here is where the average finally appears: the sum of all the response
    // times, divided by how many there were. It was never stored anywhere.
    double average = 0;
    if (stats.total > 0) average = to_ms(stats.total_time) / stats.total;
    printf("AVERAGE_RESPONSE_TIME %.6f\n", average);

    // With no records at all there is no smallest or biggest, so print 0.
    printf("MIN_RESPONSE_TIME %.6f\n", stats.total > 0 ? to_ms(stats.min_time) : 0.0);
    printf("MAX_RESPONSE_TIME %.6f\n", stats.total > 0 ? to_ms(stats.max_time) : 0.0);

    printf("TOTAL_BYTES %lld\n", stats.total_bytes);
    printf("STATUS_2XX %lld\n", stats.count_2xx);
    printf("STATUS_3XX %lld\n", stats.count_3xx);
    printf("STATUS_4XX %lld\n", stats.count_4xx);
    printf("STATUS_5XX %lld\n", stats.count_5xx);

    // The busiest 60-second interval: the minute with the most requests. The map
    // is already in order by minute, and we only replace the best on a strictly
    // bigger count, so if two minutes tie, the earlier one wins.
    long long best_minute = 0;
    long long best_count = 0;
    for (const auto& it : stats.intervals) {
        if (it.second > best_count) {
            best_count = it.second;
            best_minute = it.first;
        }
    }
    printf("BUSIEST_INTERVAL %lld %lld\n", best_minute, best_count);

    // Top K servers: id, how many requests, and their average response time.
    vector<TopRow> rows;
    for (const auto& it : stats.servers)
        rows.push_back(TopRow{it.first, it.second.count, it.second.total_time});

    printf("TOP_SERVERS\n");
    for (const auto& row : best_k(rows, k))
        printf("%lld %lld %.6f\n", row.id, row.count, to_ms(row.value) / row.count);

    // Top K endpoints: id, how many requests, and their total bytes.
    rows.clear();
    for (const auto& it : stats.endpoints)
        rows.push_back(TopRow{it.first, it.second.count, it.second.total_bytes});

    printf("TOP_ENDPOINTS\n");
    for (const auto& row : best_k(rows, k))
        printf("%lld %lld %lld\n", row.id, row.count, row.value);
}

int main(int argc, char** argv)
{
    int k_from_command_line = -1;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--k") == 0 && i + 1 < argc)
            k_from_command_line = atoi(argv[i + 1]);
    }

    // Add every line from every mapper into one Stats.
    Stats stats;
    int k = -1;
    string line;
    while (getline(cin, line)) {
        if (!line.empty()) read_pair(line, stats, k);
    }

    if (k_from_command_line >= 0) k = k_from_command_line;
    if (k < 0) k = 10;              // no mapper saw the header line

    print_report(stats, k);
    return 0;
}
