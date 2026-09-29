// gen_dataset.cpp - makes a fake log file so we can test with big inputs.
//
// We give it a "seed" number. The same seed always produces exactly the same
// file, so anyone can rebuild our test data and get the same results.
// (This works because srand(seed) makes rand() produce the same sequence
// every time on the same computer.)
//
// Run it like this:
//     ./gen_dataset 1000000 10 128 2002 data/medium.in
//                   N       K  S   seed  output file

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char** argv)
{
    if (argc < 6) {
        std::cerr << "usage: " << argv[0] << " N K S seed output_file\n";
        return 1;
    }

    long long n  = std::atoll(argv[1]);
    int k        = std::atoi(argv[2]);
    int s        = std::atoi(argv[3]);
    int seed     = std::atoi(argv[4]);
    std::string filename = argv[5];

    if (s < 1) s = 1;

    // We use C-style FILE* here instead of std::ofstream because printing
    // fifty million lines is much faster this way.
    FILE* fp = std::fopen(filename.c_str(), "w");
    if (fp == NULL) {
        std::cerr << "error: cannot write to " << filename << "\n";
        return 1;
    }

    std::srand(seed);

    // first line of the file
    std::fprintf(fp, "%lld %d %d\n", n, k, s);

    long long timestamp = 1700000000;   // a fixed starting time
    int burst_left = 0;                 // records left in a busy burst

    for (long long i = 0; i < n; i++) {
        // ---- the time of this request ----
        // Normally time moves forward by 0 to 3 seconds per request.
        // Sometimes we start a "burst": a lot of requests in a very short
        // time. We do this so that the BUSIEST_INTERVAL answer is a clear
        // winner instead of every interval having almost the same count.
        if (burst_left > 0) {
            burst_left--;
            if (i % 60 == 0) timestamp += 1;          // time moves very slowly
        } else {
            if (std::rand() % 2000 == 0) burst_left = 3000;   // start a burst
            timestamp += std::rand() % 4;
        }

        // ---- which server ----
        // Half the time we pick any server, half the time we pick from only
        // the first few. That makes some servers much busier than others,
        // so the "top K servers" list has a real winner.
        int server;
        if (std::rand() % 2 == 0) server = std::rand() % s;
        else                      server = std::rand() % (s / 4 + 1);

        // ---- which endpoint (same idea) ----
        int endpoint;
        if (std::rand() % 2 == 0) endpoint = std::rand() % (s * 8);
        else                      endpoint = std::rand() % (s / 2 + 1);

        int user = std::rand() % 100000;

        // ---- status code: mostly OK, sometimes an error ----
        int status;
        int r = std::rand() % 100;
        if      (r < 85) status = 200;
        else if (r < 92) status = 301;
        else if (r < 98) status = 404;
        else             status = 500;

        // ---- response time in milliseconds, from 0.001 to about 1000 ----
        double response_time = (std::rand() % 1000000) / 1000.0;
        if (response_time < 0.001) response_time = 0.001;

        // ---- bytes sent. Errors send very little data. ----
        // These numbers are large on purpose: added up over fifty million
        // records the total goes past 2 billion, which proves we really
        // need a 64-bit "long long" and not a normal 32-bit "int".
        long long bytes;
        if (status >= 400) bytes = 100 + std::rand() % 500;
        else               bytes = 500 + std::rand() % 100000;

        std::fprintf(fp, "%lld %d %d %d %d %.3f %lld\n",
                     timestamp, server, endpoint, user, status,
                     response_time, bytes);
    }

    std::fclose(fp);
    std::cout << "wrote " << n << " records to " << filename
              << " (seed " << seed << ")\n";
    return 0;
}
