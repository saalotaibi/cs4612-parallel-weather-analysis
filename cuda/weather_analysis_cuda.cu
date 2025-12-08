#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <float.h>
#include <sys/time.h>
#include <cuda_runtime.h>

#define MAX_CITIES 2000
#define MAX_LINE 1024
#define MAX_NAME 128
#define MAX_RECORDS_PER_FILE 50000
#define BLOCK_SIZE 256
#define WARP_SIZE 32

// Error checking macro
#define CUDA_CHECK(call) \
    do { \
        cudaError_t err = call; \
        if (err != cudaSuccess) { \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", \
                    __FILE__, __LINE__, cudaGetErrorString(err)); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

typedef struct {
    char name[MAX_NAME];
    double temp_sum;
    double temp_min;
    double temp_max;
    double precip_sum;
    int temp_count;
    int precip_count;
    int record_count;
    double monthly_temp_sum[12];
    int monthly_temp_count[12];
} CityStats;

// Parsed weather record for GPU processing
typedef struct {
    float avg_temp;
    float precipitation;
    int month;
    char valid_temp;
    char valid_precip;
} WeatherRecord;

static CityStats cities[MAX_CITIES];
static int city_count = 0;

double get_time_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Parse CSV field
char* get_field(char* line, int field_num, char* buffer, int buf_size) {
    int current_field = 0;
    char* start = line;
    char* end;

    while (current_field < field_num) {
        start = strchr(start, ',');
        if (!start) {
            buffer[0] = '\0';
            return buffer;
        }
        start++;
        current_field++;
    }

    end = strchr(start, ',');
    if (!end) {
        end = strchr(start, '\n');
        if (!end) end = start + strlen(start);
    }

    int len = end - start;
    if (len >= buf_size) len = buf_size - 1;
    strncpy(buffer, start, len);
    buffer[len] = '\0';

    return buffer;
}

// Extract month from date
int get_month(const char* date) {
    if (strlen(date) < 7) return -1;
    char month_str[3] = {date[5], date[6], '\0'};
    return atoi(month_str) - 1;
}

// Warp-level reduction for min
__device__ float warp_reduce_min(float val) {
    for (int offset = WARP_SIZE/2; offset > 0; offset /= 2) {
        float other = __shfl_down_sync(0xffffffff, val, offset);
        val = fminf(val, other);
    }
    return val;
}

// Warp-level reduction for max
__device__ float warp_reduce_max(float val) {
    for (int offset = WARP_SIZE/2; offset > 0; offset /= 2) {
        float other = __shfl_down_sync(0xffffffff, val, offset);
        val = fmaxf(val, other);
    }
    return val;
}

// Warp-level reduction for sum
__device__ float warp_reduce_sum(float val) {
    for (int offset = WARP_SIZE/2; offset > 0; offset /= 2) {
        val += __shfl_down_sync(0xffffffff, val, offset);
    }
    return val;
}

/**
 * CUDA Kernel: Process weather records for a single city
 *
 * Uses parallel reduction to aggregate:
 * - Temperature statistics (sum, min, max, count)
 * - Monthly temperature patterns
 * - Precipitation totals
 *
 * Each thread block processes one city's records
 * Shared memory used for fast intra-block reduction
 */
__global__ void process_weather_records(
    WeatherRecord* records,
    int num_records,
    double* temp_sum_out,
    double* temp_min_out,
    double* temp_max_out,
    int* temp_count_out,
    double* precip_sum_out,
    int* precip_count_out,
    double* monthly_temp_sum_out,
    int* monthly_temp_count_out
) {
    __shared__ float sh_temp_sum[BLOCK_SIZE];
    __shared__ float sh_temp_min[BLOCK_SIZE];
    __shared__ float sh_temp_max[BLOCK_SIZE];
    __shared__ int sh_temp_count[BLOCK_SIZE];
    __shared__ float sh_precip_sum[BLOCK_SIZE];
    __shared__ int sh_precip_count[BLOCK_SIZE];
    __shared__ float sh_monthly_temp[12][BLOCK_SIZE];
    __shared__ int sh_monthly_count[12][BLOCK_SIZE];

    int tid = threadIdx.x;
    int idx = blockIdx.x * blockDim.x + threadIdx.x;

    // Initialize shared memory
    sh_temp_sum[tid] = 0.0f;
    sh_temp_min[tid] = FLT_MAX;
    sh_temp_max[tid] = -FLT_MAX;
    sh_temp_count[tid] = 0;
    sh_precip_sum[tid] = 0.0f;
    sh_precip_count[tid] = 0;

    for (int m = 0; m < 12; m++) {
        sh_monthly_temp[m][tid] = 0.0f;
        sh_monthly_count[m][tid] = 0;
    }

    // Process records assigned to this thread
    while (idx < num_records) {
        WeatherRecord rec = records[idx];

        if (rec.valid_temp) {
            sh_temp_sum[tid] += rec.avg_temp;
            sh_temp_min[tid] = fminf(sh_temp_min[tid], rec.avg_temp);
            sh_temp_max[tid] = fmaxf(sh_temp_max[tid], rec.avg_temp);
            sh_temp_count[tid]++;

            if (rec.month >= 0 && rec.month < 12) {
                sh_monthly_temp[rec.month][tid] += rec.avg_temp;
                sh_monthly_count[rec.month][tid]++;
            }
        }

        if (rec.valid_precip) {
            sh_precip_sum[tid] += rec.precipitation;
            sh_precip_count[tid]++;
        }

        idx += blockDim.x * gridDim.x;
    }

    __syncthreads();

    // Block-level reduction
    for (int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (tid < s) {
            sh_temp_sum[tid] += sh_temp_sum[tid + s];
            sh_temp_min[tid] = fminf(sh_temp_min[tid], sh_temp_min[tid + s]);
            sh_temp_max[tid] = fmaxf(sh_temp_max[tid], sh_temp_max[tid + s]);
            sh_temp_count[tid] += sh_temp_count[tid + s];
            sh_precip_sum[tid] += sh_precip_sum[tid + s];
            sh_precip_count[tid] += sh_precip_count[tid + s];

            for (int m = 0; m < 12; m++) {
                sh_monthly_temp[m][tid] += sh_monthly_temp[m][tid + s];
                sh_monthly_count[m][tid] += sh_monthly_count[m][tid + s];
            }
        }
        __syncthreads();
    }

    // Thread 0 writes block result
    if (tid == 0) {
        atomicAdd(temp_sum_out, (double)sh_temp_sum[0]);
        *temp_min_out = fminf(*temp_min_out, sh_temp_min[0]);
        *temp_max_out = fmaxf(*temp_max_out, sh_temp_max[0]);
        atomicAdd(temp_count_out, sh_temp_count[0]);
        atomicAdd(precip_sum_out, (double)sh_precip_sum[0]);
        atomicAdd(precip_count_out, sh_precip_count[0]);

        for (int m = 0; m < 12; m++) {
            atomicAdd(&monthly_temp_sum_out[m], (double)sh_monthly_temp[m][0]);
            atomicAdd(&monthly_temp_count_out[m], sh_monthly_count[m][0]);
        }
    }
}

/**
 * Process a single city file using CUDA acceleration
 *
 * Steps:
 * 1. Read CSV file on CPU and parse into WeatherRecord array
 * 2. Transfer parsed records to GPU device memory
 * 3. Launch CUDA kernel to aggregate statistics in parallel
 * 4. Copy results back to CPU
 */
void process_city_file_cuda(const char* filepath, const char* city_name) {
    FILE* fp = fopen(filepath, "r");
    if (!fp) return;

    // Allocate host memory for parsed records
    WeatherRecord* h_records = (WeatherRecord*)malloc(MAX_RECORDS_PER_FILE * sizeof(WeatherRecord));
    int num_records = 0;

    char line[MAX_LINE];
    char field_buf[64];

    // Skip header
    if (!fgets(line, MAX_LINE, fp)) {
        fclose(fp);
        free(h_records);
        return;
    }

    // Parse CSV records on CPU
    while (fgets(line, MAX_LINE, fp) && num_records < MAX_RECORDS_PER_FILE) {
        WeatherRecord* rec = &h_records[num_records];
        rec->valid_temp = 0;
        rec->valid_precip = 0;

        // Get date for month
        char date[32];
        get_field(line, 2, date, sizeof(date));
        rec->month = get_month(date);

        // Get average temperature
        get_field(line, 4, field_buf, sizeof(field_buf));
        if (field_buf[0] != '\0') {
            rec->avg_temp = atof(field_buf);
            rec->valid_temp = 1;
        }

        // Get precipitation
        get_field(line, 7, field_buf, sizeof(field_buf));
        if (field_buf[0] != '\0') {
            rec->precipitation = atof(field_buf);
            rec->valid_precip = 1;
        }

        num_records++;
    }
    fclose(fp);

    if (num_records == 0) {
        free(h_records);
        return;
    }

    // Allocate device memory
    WeatherRecord* d_records;
    double *d_temp_sum, *d_temp_min, *d_temp_max;
    int *d_temp_count;
    double *d_precip_sum;
    int *d_precip_count;
    double *d_monthly_temp_sum;
    int *d_monthly_temp_count;

    CUDA_CHECK(cudaMalloc(&d_records, num_records * sizeof(WeatherRecord)));
    CUDA_CHECK(cudaMalloc(&d_temp_sum, sizeof(double)));
    CUDA_CHECK(cudaMalloc(&d_temp_min, sizeof(double)));
    CUDA_CHECK(cudaMalloc(&d_temp_max, sizeof(double)));
    CUDA_CHECK(cudaMalloc(&d_temp_count, sizeof(int)));
    CUDA_CHECK(cudaMalloc(&d_precip_sum, sizeof(double)));
    CUDA_CHECK(cudaMalloc(&d_precip_count, sizeof(int)));
    CUDA_CHECK(cudaMalloc(&d_monthly_temp_sum, 12 * sizeof(double)));
    CUDA_CHECK(cudaMalloc(&d_monthly_temp_count, 12 * sizeof(int)));

    // Initialize device memory
    double init_sum = 0.0, init_min = DBL_MAX, init_max = -DBL_MAX;
    int init_count = 0;
    double init_monthly[12] = {0};
    int init_monthly_count[12] = {0};

    CUDA_CHECK(cudaMemcpy(d_records, h_records, num_records * sizeof(WeatherRecord), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_temp_sum, &init_sum, sizeof(double), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_temp_min, &init_min, sizeof(double), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_temp_max, &init_max, sizeof(double), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_temp_count, &init_count, sizeof(int), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_precip_sum, &init_sum, sizeof(double), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_precip_count, &init_count, sizeof(int), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_monthly_temp_sum, init_monthly, 12 * sizeof(double), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_monthly_temp_count, init_monthly_count, 12 * sizeof(int), cudaMemcpyHostToDevice));

    // Launch kernel
    int num_blocks = (num_records + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (num_blocks > 128) num_blocks = 128; // Limit for efficiency

    process_weather_records<<<num_blocks, BLOCK_SIZE>>>(
        d_records, num_records,
        d_temp_sum, d_temp_min, d_temp_max, d_temp_count,
        d_precip_sum, d_precip_count,
        d_monthly_temp_sum, d_monthly_temp_count
    );

    CUDA_CHECK(cudaGetLastError());
    CUDA_CHECK(cudaDeviceSynchronize());

    // Copy results back
    CityStats* city = &cities[city_count];
    strncpy(city->name, city_name, MAX_NAME - 1);
    city->name[MAX_NAME - 1] = '\0';

    CUDA_CHECK(cudaMemcpy(&city->temp_sum, d_temp_sum, sizeof(double), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(&city->temp_min, d_temp_min, sizeof(double), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(&city->temp_max, d_temp_max, sizeof(double), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(&city->temp_count, d_temp_count, sizeof(int), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(&city->precip_sum, d_precip_sum, sizeof(double), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(&city->precip_count, d_precip_count, sizeof(int), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(city->monthly_temp_sum, d_monthly_temp_sum, 12 * sizeof(double), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(city->monthly_temp_count, d_monthly_temp_count, 12 * sizeof(int), cudaMemcpyDeviceToHost));

    city->record_count = num_records;

    // Cleanup
    cudaFree(d_records);
    cudaFree(d_temp_sum);
    cudaFree(d_temp_min);
    cudaFree(d_temp_max);
    cudaFree(d_temp_count);
    cudaFree(d_precip_sum);
    cudaFree(d_precip_count);
    cudaFree(d_monthly_temp_sum);
    cudaFree(d_monthly_temp_count);
    free(h_records);

    city_count++;
}

void print_results(void) {
    printf("\n========== WEATHER ANALYSIS RESULTS ==========\n\n");

    // Sort by average temperature (descending)
    for (int i = 0; i < city_count - 1; i++) {
        for (int j = i + 1; j < city_count; j++) {
            double avg_i = cities[i].temp_count > 0 ? cities[i].temp_sum / cities[i].temp_count : -999;
            double avg_j = cities[j].temp_count > 0 ? cities[j].temp_sum / cities[j].temp_count : -999;
            if (avg_j > avg_i) {
                CityStats temp = cities[i];
                cities[i] = cities[j];
                cities[j] = temp;
            }
        }
    }

    // Top 10 hottest cities
    printf("TOP 10 HOTTEST CITIES (by average temperature):\n");
    printf("%-25s %10s %10s %10s %12s\n", "City", "Avg(°C)", "Min(°C)", "Max(°C)", "Records");
    printf("--------------------------------------------------------------------------------\n");
    for (int i = 0; i < 10 && i < city_count; i++) {
        CityStats* c = &cities[i];
        if (c->temp_count > 0) {
            printf("%-25s %10.2f %10.2f %10.2f %12d\n",
                   c->name,
                   c->temp_sum / c->temp_count,
                   c->temp_min,
                   c->temp_max,
                   c->record_count);
        }
    }

    // Top 10 coldest cities
    printf("\nTOP 10 COLDEST CITIES (by average temperature):\n");
    printf("%-25s %10s %10s %10s %12s\n", "City", "Avg(°C)", "Min(°C)", "Max(°C)", "Records");
    printf("--------------------------------------------------------------------------------\n");
    for (int i = city_count - 1; i >= city_count - 10 && i >= 0; i--) {
        CityStats* c = &cities[i];
        if (c->temp_count > 0) {
            printf("%-25s %10.2f %10.2f %10.2f %12d\n",
                   c->name,
                   c->temp_sum / c->temp_count,
                   c->temp_min,
                   c->temp_max,
                   c->record_count);
        }
    }

    // Top 10 wettest cities
    printf("\nTOP 10 WETTEST CITIES (by total precipitation):\n");
    printf("%-25s %15s %12s\n", "City", "Total(mm)", "Days w/Rain");
    printf("--------------------------------------------------------------------------------\n");

    // Re-sort by precipitation
    for (int i = 0; i < city_count - 1; i++) {
        for (int j = i + 1; j < city_count; j++) {
            if (cities[j].precip_sum > cities[i].precip_sum) {
                CityStats temp = cities[i];
                cities[i] = cities[j];
                cities[j] = temp;
            }
        }
    }

    for (int i = 0; i < 10 && i < city_count; i++) {
        CityStats* c = &cities[i];
        printf("%-25s %15.2f %12d\n", c->name, c->precip_sum, c->precip_count);
    }

    // Overall statistics
    printf("\n========== OVERALL STATISTICS ==========\n");
    long total_records = 0;
    double global_temp_sum = 0;
    int global_temp_count = 0;

    for (int i = 0; i < city_count; i++) {
        total_records += cities[i].record_count;
        global_temp_sum += cities[i].temp_sum;
        global_temp_count += cities[i].temp_count;
    }

    printf("Total cities analyzed: %d\n", city_count);
    printf("Total records processed: %ld\n", total_records);
    printf("Global average temperature: %.2f°C\n",
           global_temp_count > 0 ? global_temp_sum / global_temp_count : 0);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s <data_directory> [max_cities]\n", argv[0]);
        printf("Example: %s ../data/cities 100\n", argv[0]);
        return 1;
    }

    const char* data_dir = argv[1];
    int max_cities = MAX_CITIES;
    if (argc >= 3) {
        max_cities = atoi(argv[2]);
    }

    // Check CUDA availability
    int device_count;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count == 0) {
        fprintf(stderr, "No CUDA devices found!\n");
        return 1;
    }

    cudaDeviceProp prop;
    CUDA_CHECK(cudaGetDeviceProperties(&prop, 0));
    printf("Weather Analysis - CUDA Version\n");
    printf("CUDA Device: %s (Compute %d.%d)\n", prop.name, prop.major, prop.minor);
    printf("Data directory: %s\n", data_dir);
    printf("Max cities: %d\n", max_cities);

    double start_time = get_time_sec();

    // Read all CSV files from directory
    DIR* dir = opendir(data_dir);
    if (!dir) {
        perror("Failed to open directory");
        return 1;
    }

    struct dirent* entry;
    int files_processed = 0;

    while ((entry = readdir(dir)) != NULL && city_count < max_cities) {
        // Check for .csv extension
        char* ext = strrchr(entry->d_name, '.');
        if (!ext || strcmp(ext, ".csv") != 0) continue;

        // Build full path
        char filepath[512];
        snprintf(filepath, sizeof(filepath), "%s/%s", data_dir, entry->d_name);

        // Extract city name
        char city_name[MAX_NAME];
        strncpy(city_name, entry->d_name, MAX_NAME - 1);
        city_name[MAX_NAME - 1] = '\0';
        char* dot = strrchr(city_name, '.');
        if (dot) *dot = '\0';

        // Replace underscores with spaces
        for (char* p = city_name; *p; p++) {
            if (*p == '_') *p = ' ';
        }

        process_city_file_cuda(filepath, city_name);
        files_processed++;

        if (files_processed % 100 == 0) {
            printf("Processed %d cities...\n", files_processed);
        }
    }

    closedir(dir);

    double end_time = get_time_sec();
    double elapsed = end_time - start_time;

    print_results();

    printf("\n========== PERFORMANCE ==========\n");
    printf("Processing time: %.3f seconds\n", elapsed);
    printf("Cities processed: %d\n", city_count);
    printf("Throughput: %.2f cities/second\n", city_count / elapsed);

    return 0;
}
