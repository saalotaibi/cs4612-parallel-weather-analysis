# Parallel Weather Analysis System

A high-performance weather data analysis system implemented in C using serial, OpenMP (shared-memory), MPI (distributed), and CUDA (GPU) approaches.

## Abstract

This project analyzes 40 years of global weather data (1983-2023) from 1,234 cities worldwide using parallel and distributed computing techniques. The system computes temperature statistics, precipitation patterns, and climate trends across 27.6 million weather records. Four implementations demonstrate different parallelization strategies: a serial baseline, OpenMP for shared-memory parallelism, MPI for distributed computing, and CUDA for GPU acceleration. Performance benchmarks show up to 7.4x speedup with OpenMP and 6.5x with MPI on 8 workers, with GPU implementation achieving additional acceleration for compute-intensive operations.

## Dataset

**Source:** [Global Daily Climate Data on Kaggle](https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data)

**Details:**
- Size: 1.5 GB (27.6 million records)
- Cities: 1,234 worldwide
- Time span: 1983-2023
- Format: CSV files (one per city)

**Installation:**
1. Download the dataset from Kaggle (requires account)
2. Extract to `data/cities/` directory
3. Each city should be a separate CSV file

## Prerequisites

```bash
# Ubuntu/Debian
sudo apt install gcc libopenmpi-dev openmpi-bin

# For CUDA (optional, requires NVIDIA GPU)
# Install CUDA Toolkit from https://developer.nvidia.com/cuda-downloads
```

## Build Instructions

### Using Makefile (Recommended)

```bash
make          # Build all versions (serial, OpenMP, MPI)
make serial   # Build serial version only
make omp      # Build OpenMP version only
make mpi      # Build MPI version only
make cuda     # Build CUDA version only
make clean    # Remove all binaries
make help     # Show help
```

### Manual Build

```bash
# Serial version
cd serial
gcc -O2 -o weather_analysis weather_analysis.c -lm

# OpenMP version (shared-memory parallel)
cd ../parallel_omp
gcc -O2 -fopenmp -o weather_analysis_omp weather_analysis_omp.c -lm

# MPI version (distributed parallel)
cd ../distributed_mpi
mpicc -O2 -o weather_analysis_mpi weather_analysis_mpi.c -lm

# CUDA version (GPU-accelerated)
cd ../cuda
nvcc -O2 -o weather_analysis_cuda weather_analysis_cuda.cu
```

## Usage

```bash
# Serial baseline
./serial/weather_analysis data/cities 1234

# OpenMP (8 threads, dynamic scheduling)
./parallel_omp/weather_analysis_omp data/cities 1234 8 dynamic

# MPI (8 processes, blocking communication)
mpirun -np 8 ./distributed_mpi/weather_analysis_mpi data/cities 1234 blocking

# CUDA (GPU acceleration)
./cuda/weather_analysis_cuda data/cities 1234
```

## Running Experiments

To reproduce the performance experiments:

```bash
./scripts/run_experiments.sh
```

This script runs multiple trials with different thread counts, process counts, and scheduling strategies, generating performance metrics in the `results/` directory.

## Project Structure

```
├── serial/                  # Serial baseline implementation
│   └── weather_analysis.c
├── parallel_omp/            # OpenMP shared-memory version
│   └── weather_analysis_omp.c
├── distributed_mpi/         # MPI distributed version
│   └── weather_analysis_mpi.c
├── cuda/                    # CUDA GPU-accelerated version
│   └── weather_analysis_cuda.cu
├── scripts/                 # Experiment scripts
│   └── run_experiments.sh
├── Makefile                 # Build automation
├── final_report_corrected.pdf
└── README.md
```

## Performance Highlights

| Implementation | Workers | Time    | Speedup | Efficiency |
|----------------|---------|---------|---------|------------|
| Serial         | 1       | 6.75s   | 1.0x    | -          |
| OpenMP         | 8       | 0.90s   | 7.4x    | 93%        |
| MPI            | 8       | 1.03s   | 6.5x    | 82%        |
| CUDA           | GPU     | ~0.5s   | ~13x    | -          |

## License

- **Code:** MIT License
- **Dataset:** CC BY-NC 4.0 (Kaggle - Global Daily Climate Data)

## Course

CS4612 Parallel & Distributed Computing - Final Project
