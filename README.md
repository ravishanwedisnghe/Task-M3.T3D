# SIT315 M3.T3D Traffic Simulator

An MPI traffic simulator that prints the top N busiest traffic lights each hour.

## Files
- traffic_mpi.cpp — MPI program
- traffic_sim.cpp — sequential comparison
- generate_data.cpp — input generator

## Compile and run
mpic++ -std=c++17 -O3 -pthread traffic_mpi.cpp -o traffic_mpi
mpirun --bind-to none -np 2 ./traffic_mpi data/traffic_handcheck.csv 3 1 1 1024

Requires Open MPI and a C++17 compiler. Test results and screenshots are included in the report.
