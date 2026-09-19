# Makefile for HW3 Q2 - real-time log analytics with gRPC
#
#   make            generate the Python gRPC stubs + build the HW2 C++ tools
#   make proto      regenerate src/loganalytics_pb2*.py from proto/loganalytics.proto
#   make tools      build reference/log_seq (HW2 sequential program) and gen_dataset
#   make data       generate the benchmark datasets (fixed seeds)
#   make verify     correctness check: gRPC system vs HW2 sequential program
#   make clean

PYTHON   ?= python3
CXX      ?= g++
CXXFLAGS  = -O2 -Wall -std=c++11

all: proto tools

proto:
	$(PYTHON) -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/loganalytics.proto

src/loganalytics_pb2.py: proto/loganalytics.proto

src/loganalytics_pb2.py: proto/loganalytics.proto
	$(PYTHON) -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/loganalytics.proto

tools: bin/log_seq bin/gen_dataset

bin/log_seq: reference/log_seq.cpp reference/common.cpp reference/common.h
	mkdir -p bin
	$(CXX) $(CXXFLAGS) -o $@ reference/log_seq.cpp reference/common.cpp

bin/gen_dataset: reference/gen_dataset.cpp
	mkdir -p bin
	$(CXX) $(CXXFLAGS) -o $@ reference/gen_dataset.cpp

data: bin/gen_dataset
	bash scripts/make_data.sh

verify: all
	$(PYTHON) scripts/verify.py

clean:
	rm -rf bin src/__pycache__

.PHONY: all proto tools data verify bench clean

bench: all
	$(PYTHON) scripts/bench.py --exp all --repeats 3
	$(PYTHON) scripts/plot.py
