// Combiner - adds up the mapper's lines and prints the same kind of lines.
//
// Input and output look identical, so the reducer cannot tell whether the
// combiner ran. That is allowed because adding can be done in any order.

#include "analytics.h"
#include <iostream>

int main()
{
    Stats s;
    int k = -1;
    string line;

    while (getline(cin, line))
        if (!line.empty()) read_pair(line, s, k);

    print_pairs(s, k);
    return 0;
}
