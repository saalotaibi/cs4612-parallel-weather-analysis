CC = gcc
MPICC = mpicc
NVCC = nvcc
CFLAGS = -O2 -Wall
OMPFLAGS = -fopenmp
LIBS = -lm

SERIAL_DIR = serial
OMP_DIR = parallel_omp
MPI_DIR = distributed_mpi
CUDA_DIR = cuda

SERIAL_BIN = $(SERIAL_DIR)/weather_analysis
OMP_BIN = $(OMP_DIR)/weather_analysis_omp
MPI_BIN = $(MPI_DIR)/weather_analysis_mpi
CUDA_BIN = $(CUDA_DIR)/weather_analysis_cuda

.PHONY: all serial omp mpi cuda clean help

all: serial omp mpi
	@echo "All implementations built successfully!"

serial:
	@echo "Building serial version..."
	$(CC) $(CFLAGS) -o $(SERIAL_BIN) $(SERIAL_DIR)/weather_analysis.c $(LIBS)
	@echo "Serial version built: $(SERIAL_BIN)"

omp:
	@echo "Building OpenMP version..."
	$(CC) $(CFLAGS) $(OMPFLAGS) -o $(OMP_BIN) $(OMP_DIR)/weather_analysis_omp.c $(LIBS)
	@echo "OpenMP version built: $(OMP_BIN)"

mpi:
	@echo "Building MPI version..."
	$(MPICC) $(CFLAGS) -o $(MPI_BIN) $(MPI_DIR)/weather_analysis_mpi.c $(LIBS)
	@echo "MPI version built: $(MPI_BIN)"

cuda:
	@echo "Building CUDA version..."
	$(NVCC) -O2 -o $(CUDA_BIN) $(CUDA_DIR)/weather_analysis_cuda.cu
	@echo "CUDA version built: $(CUDA_BIN)"

clean:
	@echo "Cleaning binaries..."
	rm -f $(SERIAL_BIN) $(OMP_BIN) $(MPI_BIN) $(CUDA_BIN)
	@echo "Clean complete!"

help:
	@echo "Parallel Weather Analysis System - Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make              - Build serial, OpenMP, and MPI versions"
	@echo "  make all          - Same as 'make'"
	@echo "  make serial       - Build serial version only"
	@echo "  make omp          - Build OpenMP version only"
	@echo "  make mpi          - Build MPI version only"
	@echo "  make cuda         - Build CUDA version (requires CUDA toolkit)"
	@echo "  make clean        - Remove all compiled binaries"
	@echo "  make help         - Show this help message"
	@echo ""
	@echo "Running the programs:"
	@echo "  Serial:  ./serial/weather_analysis data/cities 1234"
	@echo "  OpenMP:  ./parallel_omp/weather_analysis_omp data/cities 1234 8 dynamic"
	@echo "  MPI:     mpirun -np 8 ./distributed_mpi/weather_analysis_mpi data/cities 1234 blocking"
	@echo "  CUDA:    ./cuda/weather_analysis_cuda data/cities 1234"
