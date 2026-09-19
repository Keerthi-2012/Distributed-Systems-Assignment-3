// log_seq.cpp - the SEQUENTIAL version (one processor only).
//
// This is the simple, normal program. It does everything by itself:
//   1. read the whole file
//   2. look at every record and count things
//   3. print the answers
//
// We also use it to check that the MPI version is correct: both programs
// must print exactly the same answers for the same input file.
//
// Run it like this:
//     ./log_seq data/medium.in

#include "common.h"

#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv)
{
    std::string filename;
    bool show_time = false;

    // ---- read the command line ----
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--time") == 0) show_time = true;
        else filename = argv[i];
    }
    if (filename.empty()) {
        std::cerr << "usage: " << argv[0] << " <input_file> [--time]\n";
        return 1;
    }

    std::ifstream file(filename.c_str());
    if (!file) {
        std::cerr << "error: cannot open file " << filename << "\n";
        return 1;
    }

    // ---- first line of the file is:  N K S ----
    long long n = 0;
    int k = 0, s = 0;
    std::string line;
    if (!std::getline(file, line) ||
        std::sscanf(line.c_str(), "%lld %d %d", &n, &k, &s) != 3) {
        std::cerr << "error: first line should be: N K S\n";
        return 1;
    }

    clock_t start = clock();

    // ---- read all the records ----
    std::vector<Record> recs;
    recs.reserve(n > 0 ? n : 1);          // ask for the memory once, up front

    for (long long i = 0; i < n; i++) {
        if (!std::getline(file, line)) {
            std::cerr << "error: file ended early, only " << i << " records\n";
            return 1;
        }
        Record r;
        if (!parse_line(line, r)) {
            std::cerr << "error: bad data on record " << (i + 1) << "\n";
            return 1;
        }
        recs.push_back(r);
    }
    file.close();

    // ---- find out how big our vectors need to be ----
    // We must know the largest server id, the largest endpoint id, and the
    // range of timestamps BEFORE we can create the counting vectors.
    int max_server, max_endpoint;
    long long min_time, max_time;
    find_ranges(recs, max_server, max_endpoint, min_time, max_time);

    // The header tells us there are S servers, but we also make sure the
    // vector is big enough for the largest id we actually saw.
    if (max_server + 1 > s) s = max_server + 1;

    long long first_interval, num_intervals;
    if (n > 0) {
        first_interval = interval_of(min_time);
        num_intervals  = interval_of(max_time) - first_interval + 1;
    } else {
        first_interval = 0;
        num_intervals  = 1;
    }

    // ---- create the vectors and count everything ----
    Stats st;
    st.create(s, max_endpoint + 1, first_interval, num_intervals);
    st.count(recs);

    clock_t stop = clock();

    // ---- print the answers ----
    st.print(k);

    // Timing goes to stderr, NOT stdout, because the assignment says the
    // program output must contain only the required lines.
    if (show_time)
        std::fprintf(stderr, "TIME %.6f seconds\n",
                     (double)(stop - start) / CLOCKS_PER_SEC);

    return 0;
}
