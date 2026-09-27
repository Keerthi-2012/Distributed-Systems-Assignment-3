# Makefile for Section 2 (HW2 Q7 as MapReduce and as gRPC).
#
#   make            reference tools + Q1 MapReduce binaries
#   make tools      reference/log_seq and reference/gen_dataset (HW2, unchanged)
#   make q1         Q1 mapper, combiner, reducer
#   make data       generate the datasets (fixed seeds)
#   make verify     check Q1 against the HW2 sequential program
#   make clean
#
# Q2's gRPC binaries are built separately with CMake, because they need the
# gRPC C++ toolchain (see Q2_grpc/ and the README).

CXX      ?= g++
CXXFLAGS  = -O2 -Wall -std=c++11
BIN       = bin

all: tools q1

$(BIN):
	mkdir -p $(BIN)

# ---- HW2 reference: the correctness baseline and the dataset generator ----
tools: $(BIN)/log_seq $(BIN)/gen_dataset

# The MPI program, for the Q1-vs-MPI comparison. Separate target because it
# needs an MPI compiler:  module load openmpi/4.1.5 && make mpi
MPICXX ?= mpic++
mpi: $(BIN)/log_mpi

$(BIN)/log_mpi: reference/log_mpi.cpp reference/common.cpp reference/common.h | $(BIN)
	$(MPICXX) $(CXXFLAGS) -o $@ reference/log_mpi.cpp reference/common.cpp

$(BIN)/log_seq: reference/log_seq.cpp reference/common.cpp reference/common.h | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ reference/log_seq.cpp reference/common.cpp

$(BIN)/gen_dataset: reference/gen_dataset.cpp | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ reference/gen_dataset.cpp

# ---- Q1: standalone C++ executables for Hadoop Streaming ----
q1: $(BIN)/mapper $(BIN)/combiner $(BIN)/reducer

COMMON_SRC = common/analytics.cpp
COMMON_HDR = common/analytics.h

$(BIN)/mapper: Q1_mapreduce/mapper.cpp $(COMMON_SRC) $(COMMON_HDR) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ Q1_mapreduce/mapper.cpp $(COMMON_SRC)

$(BIN)/combiner: Q1_mapreduce/combiner.cpp $(COMMON_SRC) $(COMMON_HDR) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ Q1_mapreduce/combiner.cpp $(COMMON_SRC)

$(BIN)/reducer: Q1_mapreduce/reducer.cpp $(COMMON_SRC) $(COMMON_HDR) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ Q1_mapreduce/reducer.cpp $(COMMON_SRC)

data: $(BIN)/gen_dataset
	bash scripts/make_data.sh

verify: all
	bash scripts/verify_q1.sh

clean:
	rm -rf $(BIN)

.PHONY: all tools mpi q1 data verify clean
