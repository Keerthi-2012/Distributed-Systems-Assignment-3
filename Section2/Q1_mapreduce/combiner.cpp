// combiner.cpp - an optional extra step between the map and the reduce.
//
//     ./mapper < piece | sort | ./combiner
//
// It reads the mapper's lines, adds them together, and prints the same kind of
// lines again. Input and output look the same, so the reducer cannot tell
// whether the combiner ran or not - that is the rule a combiner must follow.
// Here that rule is easy to keep, because it reads with read_pair() and writes
// with Stats::write(), the very same pair the mapper uses.
//
// It is allowed because adding these numbers can be done in any order and in
// any grouping, and the answer stays the same.
//
// Our mapper already adds up its own piece, so there is usually little left for
// the combiner to do. It is kept because Hadoop may run several map tasks on
// one machine, and their output still combines here before crossing the
// network.

#include "../common/analytics.h"

#include <iostream>

int main()
{
    Stats stats;
    int k = -1;
    string line;

    while (getline(cin, line)) {
        if (!line.empty()) read_pair(line, stats, k);
    }

    stats.write(k);         // the same kind of lines the mapper prints
    return 0;
}
