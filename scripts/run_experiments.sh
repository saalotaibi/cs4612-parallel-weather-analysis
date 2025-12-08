#!/bin/bash

# Weather Analysis - Experiment Runner
# Runs serial, OpenMP, and MPI experiments with various configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_DIR/data/cities"
RESULTS_DIR="$PROJECT_DIR/results"

mkdir -p "$RESULTS_DIR"

# Number of trials per experiment
TRIALS=3
MAX_CITIES=1234

echo "=============================================="
echo "Weather Analysis - Experiment Suite"
echo "=============================================="
echo "Data directory: $DATA_DIR"
echo "Trials per experiment: $TRIALS"
echo "Max cities: $MAX_CITIES"
echo ""

# Compile all versions
echo "Compiling..."
cd "$PROJECT_DIR/serial"
gcc -O2 -o weather_analysis weather_analysis.c -lm

cd "$PROJECT_DIR/parallel_omp"
gcc -O2 -fopenmp -o weather_analysis_omp weather_analysis_omp.c -lm

cd "$PROJECT_DIR/distributed_mpi"
mpicc -O2 -o weather_analysis_mpi weather_analysis_mpi.c -lm

echo "Compilation complete."
echo ""

# Function to run and time experiment
run_experiment() {
    local name=$1
    local cmd=$2
    local output_file=$3

    echo "Running: $name"
    echo "Command: $cmd"

    local times=""
    for trial in $(seq 1 $TRIALS); do
        # Run and extract time
        local result=$($cmd 2>&1 | grep "Processing time:" | awk '{print $3}')
        times="$times $result"
        echo "  Trial $trial: ${result}s"
    done

    # Calculate average
    local avg=$(echo $times | tr ' ' '\n' | awk '{sum+=$1; count++} END {printf "%.3f", sum/count}')
    echo "  Average: ${avg}s"
    echo "$name,$avg" >> "$output_file"
    echo ""
}

# ======================
# SERIAL BASELINE
# ======================
echo "=============================================="
echo "1. Serial Baseline"
echo "=============================================="

SERIAL_RESULTS="$RESULTS_DIR/serial_results.csv"
echo "experiment,time_sec" > "$SERIAL_RESULTS"

run_experiment "serial_baseline" \
    "$PROJECT_DIR/serial/weather_analysis $DATA_DIR $MAX_CITIES" \
    "$SERIAL_RESULTS"

SERIAL_TIME=$(tail -1 "$SERIAL_RESULTS" | cut -d',' -f2)

# ======================
# OPENMP SCALING
# ======================
echo "=============================================="
echo "2. OpenMP Strong Scaling"
echo "=============================================="

OMP_RESULTS="$RESULTS_DIR/openmp_results.csv"
echo "threads,schedule,time_sec,speedup,efficiency" > "$OMP_RESULTS"

for threads in 1 2 4 8; do
    for schedule in static dynamic guided; do
        echo "Running: OpenMP threads=$threads schedule=$schedule"
        times=""
        for trial in $(seq 1 $TRIALS); do
            result=$("$PROJECT_DIR/parallel_omp/weather_analysis_omp" "$DATA_DIR" $MAX_CITIES $threads $schedule 2>&1 | grep "Processing time:" | awk '{print $3}')
            times="$times $result"
            echo "  Trial $trial: ${result}s"
        done

        avg=$(echo $times | tr ' ' '\n' | awk '{sum+=$1; count++} END {printf "%.3f", sum/count}')
        speedup=$(echo "scale=3; $SERIAL_TIME / $avg" | bc)
        efficiency=$(echo "scale=3; $speedup / $threads" | bc)

        echo "  Average: ${avg}s, Speedup: ${speedup}x, Efficiency: ${efficiency}"
        echo "$threads,$schedule,$avg,$speedup,$efficiency" >> "$OMP_RESULTS"
        echo ""
    done
done

# ======================
# MPI SCALING
# ======================
echo "=============================================="
echo "3. MPI Strong Scaling"
echo "=============================================="

MPI_RESULTS="$RESULTS_DIR/mpi_results.csv"
echo "processes,comm_mode,time_sec,speedup,efficiency" > "$MPI_RESULTS"

for procs in 1 2 4 8; do
    for comm in blocking nonblocking; do
        echo "Running: MPI procs=$procs comm=$comm"
        times=""
        for trial in $(seq 1 $TRIALS); do
            result=$(mpirun --oversubscribe -np $procs "$PROJECT_DIR/distributed_mpi/weather_analysis_mpi" "$DATA_DIR" $MAX_CITIES $comm 2>&1 | grep "Processing time:" | awk '{print $3}')
            times="$times $result"
            echo "  Trial $trial: ${result}s"
        done

        avg=$(echo $times | tr ' ' '\n' | awk '{sum+=$1; count++} END {printf "%.3f", sum/count}')
        speedup=$(echo "scale=3; $SERIAL_TIME / $avg" | bc)
        efficiency=$(echo "scale=3; $speedup / $procs" | bc)

        echo "  Average: ${avg}s, Speedup: ${speedup}x, Efficiency: ${efficiency}"
        echo "$procs,$comm,$avg,$speedup,$efficiency" >> "$MPI_RESULTS"
        echo ""
    done
done

# ======================
# WEAK SCALING (varying problem size)
# ======================
echo "=============================================="
echo "4. Weak Scaling (OpenMP)"
echo "=============================================="

WEAK_RESULTS="$RESULTS_DIR/weak_scaling_results.csv"
echo "threads,cities,time_sec,cities_per_thread" > "$WEAK_RESULTS"

BASE_CITIES=100  # cities per thread

for threads in 1 2 4 8; do
    cities=$((BASE_CITIES * threads))
    if [ $cities -gt $MAX_CITIES ]; then
        cities=$MAX_CITIES
    fi

    echo "Running: Weak scaling threads=$threads cities=$cities"
    times=""
    for trial in $(seq 1 $TRIALS); do
        result=$("$PROJECT_DIR/parallel_omp/weather_analysis_omp" "$DATA_DIR" $cities $threads dynamic 2>&1 | grep "Processing time:" | awk '{print $3}')
        times="$times $result"
        echo "  Trial $trial: ${result}s"
    done

    avg=$(echo $times | tr ' ' '\n' | awk '{sum+=$1; count++} END {printf "%.3f", sum/count}')
    echo "  Average: ${avg}s"
    echo "$threads,$cities,$avg,$BASE_CITIES" >> "$WEAK_RESULTS"
    echo ""
done

echo "=============================================="
echo "Experiments Complete!"
echo "=============================================="
echo "Results saved to:"
echo "  - $SERIAL_RESULTS"
echo "  - $OMP_RESULTS"
echo "  - $MPI_RESULTS"
echo "  - $WEAK_RESULTS"
