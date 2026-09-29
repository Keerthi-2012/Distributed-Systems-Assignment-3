// log_mpi.cpp - the MPI version of the Q7 log analytics.
//
//     mpirun -np 4 ./bin/log_mpi data/large.in
//
// WHY THIS FILE EXISTS
//
// The assignment asks us to compare the MapReduce implementation (Q1) with the
// MPI implementation of the same problem from Homework 2. HW2's own log_mpi.cpp
// is not in this folder - only log_seq.cpp, common.cpp and common.h were kept -
// so this is a reconstruction. It is written against HW2's UNCHANGED
// common.h/common.cpp, so the parsing, the counting and the printing are
// literally the same code the sequential program uses. Only the distribution is
// new, and that is the thing being compared.
//
// HOW THE WORK IS DIVIDED
//
// Each rank reads a byte range of the file and nothing else:
//
//     rank r  ->  bytes [ size*r/P , size*(r+1)/P )
//
// A byte split lands in the middle of lines, so each rank skips forward to the
// next newline and then reads one line past its end. That is exactly how Hadoop
// splits a file across mappers, which keeps the comparison honest: both
// implementations read the same bytes in the same way, and neither pays a
// scatter from rank 0.
//
// HOW THE ANSWERS ARE COMBINED
//
// The same insight the whole assignment rests on: every stored value is a
// count, a sum, a min or a max, so the ranks can count independently and be
// combined at the end.
//
//     counts and sums     MPI_Reduce with MPI_SUM
//     min / max times     MPI_Reduce with MPI_MIN and MPI_MAX
//
// Rank 0 then prints with HW2's Stats::print, so the output is identical to
// log_seq by construction.
//
// The per-server, per-endpoint and per-interval tables are dense arrays, as in
// HW2, so every rank must agree on their size before reducing. That needs one
// MPI_Allreduce over the largest ids and the timestamp range first - the only
// communication before the final reduction.

#include "common.h"

#include <mpi.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    int rank = 0, procs = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &procs);

    string filename;
    bool show_time = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--time") == 0) show_time = true;
        else filename = argv[i];
    }
    if (filename.empty()) {
        if (rank == 0) cerr << "usage: " << argv[0] << " <input_file> [--time]\n";
        MPI_Finalize();
        return 1;
    }

    double t_start = MPI_Wtime();

    // ---- every rank reads the header, so everyone knows K ----
    long long n = 0;
    int k = 0, s = 0;
    {
        ifstream head(filename.c_str());
        string line;
        if (!head || !getline(head, line) ||
            sscanf(line.c_str(), "%lld %d %d", &n, &k, &s) != 3) {
            if (rank == 0) cerr << "error: first line should be: N K S\n";
            MPI_Finalize();
            return 1;
        }
    }

    // ---- work out this rank's byte range ----
    ifstream file(filename.c_str(), ios::binary);
    if (!file) {
        if (rank == 0) cerr << "error: cannot open " << filename << "\n";
        MPI_Finalize();
        return 1;
    }
    file.seekg(0, ios::end);
    long long file_size = (long long)file.tellg();

    long long begin = file_size * rank / procs;
    long long end = file_size * (rank + 1) / procs;

    file.seekg(begin);
    string line;
    // A split lands mid-line: everyone but rank 0 throws away the partial line,
    // because the rank before them will read it whole.
    if (rank != 0) getline(file, line);

    // ---- read this rank's records ----
    //
    // A line belongs to the rank whose range contains the line's START. The
    // last line of a range may run past `end`, and that is fine: the next rank
    // begins inside that same line and throws it away above. Reading an extra
    // line here would count it twice - with P ranks that is P-1 records too
    // many, which is exactly the kind of error that only shows up once the
    // total stops matching the sequential program.
    vector<Record> recs;
    recs.reserve((size_t)(n / procs + 16));
    while (file.good() && (long long)file.tellg() < end && getline(file, line)) {
        Record r;
        if (parse_line(line, r)) recs.push_back(r);   // the header fails to parse
    }
    file.close();

    // ---- agree on the table sizes before counting ----
    int local_max_server, local_max_endpoint;
    long long local_min_time, local_max_time;
    find_ranges(recs, local_max_server, local_max_endpoint, local_min_time, local_max_time);
    if (recs.empty()) {           // values that lose every comparison
        local_min_time = 0x7FFFFFFFFFFFFFFFLL;
        local_max_time = -0x7FFFFFFFFFFFFFFFLL;
    }

    int max_server = 0, max_endpoint = 0;
    long long min_time = 0, max_time = 0;
    MPI_Allreduce(&local_max_server, &max_server, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&local_max_endpoint, &max_endpoint, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&local_min_time, &min_time, 1, MPI_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
    MPI_Allreduce(&local_max_time, &max_time, 1, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);

    long long total_records = 0, local_records = (long long)recs.size();
    MPI_Allreduce(&local_records, &total_records, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

    if (max_server + 1 > s) s = max_server + 1;
    long long first_interval, num_intervals;
    if (total_records > 0) {
        first_interval = interval_of(min_time);
        num_intervals = interval_of(max_time) - first_interval + 1;
    } else {
        first_interval = 0;
        num_intervals = 1;
    }

    // ---- count locally, with HW2's own counting code ----
    Stats st;
    st.create(s, max_endpoint + 1, first_interval, num_intervals);
    st.count(recs);

    // ---- combine: sums, then min and max ----
    Stats total;
    if (rank == 0) total.create(s, max_endpoint + 1, first_interval, num_intervals);

    long long scalars[8] = {st.total, st.success, st.failed, st.count_2xx,
                            st.count_3xx, st.count_4xx, st.count_5xx, st.total_bytes};
    long long scalars_sum[8] = {0};
    MPI_Reduce(scalars, scalars_sum, 8, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    double time_sum = 0, time_min = 0, time_max = 0;
    MPI_Reduce(&st.time_sum, &time_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&st.time_min, &time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&st.time_max, &time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    MPI_Reduce(st.server_count.data(), rank == 0 ? total.server_count.data() : NULL,
               (int)st.server_count.size(), MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(st.server_time.data(), rank == 0 ? total.server_time.data() : NULL,
               (int)st.server_time.size(), MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(st.endpoint_count.data(), rank == 0 ? total.endpoint_count.data() : NULL,
               (int)st.endpoint_count.size(), MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(st.endpoint_bytes.data(), rank == 0 ? total.endpoint_bytes.data() : NULL,
               (int)st.endpoint_bytes.size(), MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(st.interval_count.data(), rank == 0 ? total.interval_count.data() : NULL,
               (int)st.interval_count.size(), MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    double t_end = MPI_Wtime();

    if (rank == 0) {
        total.total = scalars_sum[0];
        total.success = scalars_sum[1];
        total.failed = scalars_sum[2];
        total.count_2xx = scalars_sum[3];
        total.count_3xx = scalars_sum[4];
        total.count_4xx = scalars_sum[5];
        total.count_5xx = scalars_sum[6];
        total.total_bytes = scalars_sum[7];
        total.time_sum = time_sum;
        total.time_min = time_min;
        total.time_max = time_max;

        total.print(k);           // HW2's printer: identical output by construction

        // Timing goes to stderr: stdout carries only the required lines.
        if (show_time)
            fprintf(stderr, "TIME %.6f seconds with %d processes\n", t_end - t_start, procs);
    }

    MPI_Finalize();
    return 0;
}
