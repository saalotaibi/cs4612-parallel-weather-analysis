# Makefile for CUDA Weather Analysis
# Requires: NVIDIA CUDA Toolkit (tested with CUDA 11.0+)

# Compiler
NVCC = nvcc

# Compiler flags
# RTX 3050 = sm_86 (Ampere), fallback to sm_60 (Pascal) for compatibility
NVCC_FLAGS = -O3 -arch=sm_86 -Xcompiler -fopenmp
NVCC_FLAGS += -std=c++11

# Alternative: Use sm_60 for broader compatibility (GTX 10xx+)
# NVCC_FLAGS = -O3 -arch=sm_60 -Xcompiler -fopenmp

# Target executable
TARGET = weather_analysis_cuda

# Source file
SRC = weather_analysis_cuda.cu

# Default data directory
DATA_DIR = ../data/cities

.PHONY: all clean run benchmark

all: $(TARGET)

$(TARGET): $(SRC)
	$(NVCC) $(NVCC_FLAGS) -o $(TARGET) $(SRC)
	@echo "Build successful! Binary: $(TARGET)"
	@echo "Usage: ./$(TARGET) <data_directory> [max_cities]"

# Run with default settings
run: $(TARGET)
	./$(TARGET) $(DATA_DIR)

# Run benchmark with 10 Saudi cities
benchmark: $(TARGET)
	@echo "Running CUDA benchmark (10 Saudi cities)..."
	./$(TARGET) $(DATA_DIR) 10

# Run full dataset
full: $(TARGET)
	@echo "Running full dataset analysis..."
	./$(TARGET) $(DATA_DIR)

clean:
	rm -f $(TARGET)
	@echo "Clean complete"

# Help
help:
	@echo "Available targets:"
	@echo "  make          - Build the CUDA executable"
	@echo "  make run      - Run with default settings"
	@echo "  make benchmark - Run with 10 Saudi cities"
	@echo "  make full     - Run full dataset analysis"
	@echo "  make clean    - Remove compiled binaries"
	@echo ""
	@echo "Manual usage:"
	@echo "  ./$(TARGET) <data_directory> [max_cities]"
	@echo ""
	@echo "Example:"
	@echo "  ./$(TARGET) ../data/cities 100"
