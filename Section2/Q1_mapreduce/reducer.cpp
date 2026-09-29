// Reducer - adds up every mapper's lines and prints the final answer.
//
// Only ONE reducer is used, because the Top-K lists need the counts of all
// servers: a server that is 11th on every reducer could still be 1st overall.

#include "../common/analytics.h"
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

double to_ms(long long v) { return v / 1000000.0; }

// One line of a top list, so we can sort by count instead of by id.
struct Row {
    long long id, count, value;
};

// More requests first; if two are equal, the smaller id first.
bool better(const Row& a, const Row& b)
{
    if (a.count != b.count) return a.count > b.count;
    return a.id < b.id;
}

// Build the rows from two maps: one holding the counts, one the other number.
vector<Row> top_rows(const map<int, long long>& counts,
                     const map<int, long long>& values, int k)
{
    vector<Row> rows;
    for (auto& it : counts)
        rows.push_back(Row{it.first, it.second, values.at(it.first)});

    sort(rows.begin(), rows.end(), better);
    if ((int)rows.size() > k) rows.resize(k < 0 ? 0 : k);
    return rows;
}

void print_answer(const Stats& s, int k)
{
    printf("TOTAL_REQUESTS %lld\n", s.total);
    printf("SUCCESSFUL_REQUESTS %lld\n", s.success);
    printf("FAILED_REQUESTS %lld\n", s.failed);

    // The average finally appears here: the sum divided by the count.
    double average = 0;
    if (s.total > 0) average = to_ms(s.time_sum) / s.total;
    printf("AVERAGE_RESPONSE_TIME %.6f\n", average);

    printf("MIN_RESPONSE_TIME %.6f\n", s.total > 0 ? to_ms(s.time_min) : 0.0);
    printf("MAX_RESPONSE_TIME %.6f\n", s.total > 0 ? to_ms(s.time_max) : 0.0);
    printf("TOTAL_BYTES %lld\n", s.bytes);
    printf("STATUS_2XX %lld\n", s.c2xx);
    printf("STATUS_3XX %lld\n", s.c3xx);
    printf("STATUS_4XX %lld\n", s.c4xx);
    printf("STATUS_5XX %lld\n", s.c5xx);

    // Busiest minute. The map is already in order, and we only replace on a
    // strictly bigger count, so if two minutes tie the earlier one wins.
    long long best_minute = 0, best_count = 0;
    for (auto& it : s.minute_count)
        if (it.second > best_count) { best_count = it.second; best_minute = it.first; }
    printf("BUSIEST_INTERVAL %lld %lld\n", best_minute, best_count);

    printf("TOP_SERVERS\n");
    for (auto& r : top_rows(s.server_count, s.server_time, k))
        printf("%lld %lld %.6f\n", r.id, r.count, to_ms(r.value) / r.count);

    printf("TOP_ENDPOINTS\n");
    for (auto& r : top_rows(s.endpoint_count, s.endpoint_bytes, k))
        printf("%lld %lld %lld\n", r.id, r.count, r.value);
}

int main(int argc, char** argv)
{
    int k_from_command_line = -1;
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], "--k") == 0 && i + 1 < argc)
            k_from_command_line = atoi(argv[i + 1]);

    Stats s;
    int k = -1;
    string line;

    while (getline(cin, line))
        if (!line.empty()) read_pair(line, s, k);

    if (k_from_command_line >= 0) k = k_from_command_line;
    if (k < 0) k = 10;             // no mapper saw the header line

    print_answer(s, k);
    return 0;
}
