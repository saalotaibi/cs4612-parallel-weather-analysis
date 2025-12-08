#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <float.h>
#include <sys/time.h>
#include <mpi.h>

#define MAX_CITIES 2000
#define MAX_LINE 1024
#define MAX_NAME 128
#define MAX_FILES 2000

// Field indices for CSV parsing
#define FIELD_DATE 2
#define FIELD_AVG_TEMP 4
#define FIELD_PRECIP 7

typedef struct {
    char name[MAX_NAME];
    double temp_sum;
    double temp_min;
    double temp_max;
    double precip_sum;
    int temp_count;
    int precip_count;
    int record_count;
    // Monthly averages (0-11) - same as serial/OpenMP for consistency
    double monthly_temp_sum[12];
    int monthly_temp_count[12];
} CityStats;

// File list
static char file_paths[MAX_FILES][512];
static char city_names[MAX_FILES][MAX_NAME];
static int num_files = 0;

double get_time_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

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

    // Fix: validate length to prevent buffer overflow
    int len = end - start;
    if (len < 0 || end < start) {
        buffer[0] = '\0';
        return buffer;
    }
    if (len >= buf_size) len = buf_size - 1;
    strncpy(buffer, start, len);
    buffer[len] = '\0';

    return buffer;
}

// Extract month from date (YYYY-MM-DD format)
int get_month(const char* date) {
    if (strlen(date) < 7) return -1;
    char month_str[3] = {date[5], date[6], '\0'};
    int month = atoi(month_str) - 1;  // 0-indexed
    if (month < 0 || month > 11) return -1;
    return month;
}

void process_city_file(const char* filepath, CityStats* city) {
    FILE* fp = fopen(filepath, "r");
    if (!fp) return;

    city->temp_sum = 0;
    city->temp_min = DBL_MAX;
    city->temp_max = -DBL_MAX;
    city->precip_sum = 0;
    city->temp_count = 0;
    city->precip_count = 0;
    city->record_count = 0;
    memset(city->monthly_temp_sum, 0, sizeof(city->monthly_temp_sum));
    memset(city->monthly_temp_count, 0, sizeof(city->monthly_temp_count));

    char line[MAX_LINE];
    char field_buf[64];

    // Skip header
    if (!fgets(line, MAX_LINE, fp)) {
        fclose(fp);
        return;
    }

    while (fgets(line, MAX_LINE, fp)) {
        city->record_count++;

        // Get date for month extraction
        char date[32];
        get_field(line, FIELD_DATE, date, sizeof(date));
        int month = get_month(date);

        // Get average temperature
        get_field(line, FIELD_AVG_TEMP, field_buf, sizeof(field_buf));
        if (field_buf[0] != '\0') {
            double temp = atof(field_buf);
            city->temp_sum += temp;
            city->temp_count++;

            if (temp < city->temp_min) city->temp_min = temp;
            if (temp > city->temp_max) city->temp_max = temp;

            // Monthly tracking (same as serial/OpenMP)
            if (month >= 0 && month < 12) {
                city->monthly_temp_sum[month] += temp;
                city->monthly_temp_count[month]++;
            }
        }

        // Get precipitation
        get_field(line, FIELD_PRECIP, field_buf, sizeof(field_buf));
        if (field_buf[0] != '\0') {
            double precip = atof(field_buf);
            city->precip_sum += precip;
            city->precip_count++;
        }
    }

    fclose(fp);
}

void collect_files(const char* data_dir, int max_cities) {
    DIR* dir = opendir(data_dir);
    if (!dir) {
        perror("Failed to open directory");
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL && num_files < max_cities) {
        char* ext = strrchr(entry->d_name, '.');
        if (!ext || strcmp(ext, ".csv") != 0) continue;

        snprintf(file_paths[num_files], sizeof(file_paths[0]), "%s/%s", data_dir, entry->d_name);

        strncpy(city_names[num_files], entry->d_name, MAX_NAME - 1);
        city_names[num_files][MAX_NAME - 1] = '\0';
        char* dot = strrchr(city_names[num_files], '.');
        if (dot) *dot = '\0';

        for (char* p = city_names[num_files]; *p; p++) {
            if (*p == '_') *p = ' ';
        }

        num_files++;
    }

    closedir(dir);
}

void print_results(CityStats* cities, int city_count) {
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

    printf("\nTOP 10 COLDEST CITIES (by average temperature):\n");
    printf("%-25s %10s %10s %10s %12s\n", "City", "Avg(°C)", "Min(°C)", "Max(°C)", "Records");
    printf("--------------------------------------------------------------------------------\n");
    for (int i = city_count - 1; i >= 0 && i >= city_count - 10; i--) {
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

    printf("\nTOP 10 WETTEST CITIES (by total precipitation):\n");
    printf("%-25s %15s %12s\n", "City", "Total(mm)", "Days w/Rain");
    printf("--------------------------------------------------------------------------------\n");

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
    int rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0) {
            printf("Usage: mpirun -np <procs> %s <data_directory> [max_cities] [comm_mode] [dist_mode]\n", argv[0]);
            printf("  comm_mode: blocking, nonblocking (default: blocking)\n");
            printf("  dist_mode: block, cyclic (default: block)\n");
            printf("Example: mpirun -np 4 %s ../data/cities 100 blocking block\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    const char* data_dir = argv[1];
    int max_cities = MAX_CITIES;
    const char* comm_mode = "blocking";
    const char* dist_mode = "block";

    if (argc >= 3) max_cities = atoi(argv[2]);
    if (argc >= 4) comm_mode = argv[3];
    if (argc >= 5) dist_mode = argv[4];

    if (rank == 0) {
        printf("Weather Analysis - MPI Distributed Version\n");
        printf("Data directory: %s\n", data_dir);
        printf("Max cities: %d\n", max_cities);
        printf("Processes: %d\n", size);
        printf("Communication: %s\n", comm_mode);
        printf("Distribution: %s\n", dist_mode);
    }

    // All processes collect file list (simpler than broadcasting)
    collect_files(data_dir, max_cities);

    if (rank == 0) {
        printf("Files found: %d\n", num_files);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double start_time = MPI_Wtime();

    // Determine which files this process handles based on distribution mode
    int my_count = 0;
    int* my_file_indices = malloc(num_files * sizeof(int));  // Max possible

    if (strcmp(dist_mode, "cyclic") == 0) {
        // Cyclic distribution: rank 0 gets files 0, size, 2*size, ...
        //                      rank 1 gets files 1, size+1, 2*size+1, ...
        for (int i = rank; i < num_files; i += size) {
            my_file_indices[my_count++] = i;
        }
    } else {
        // Block distribution (default): contiguous chunks
        int files_per_proc = (num_files + size - 1) / size;
        int my_start = rank * files_per_proc;
        int my_end = my_start + files_per_proc;
        if (my_end > num_files) my_end = num_files;
        for (int i = my_start; i < my_end && i < num_files; i++) {
            my_file_indices[my_count++] = i;
        }
    }

    // Process local files
    CityStats* local_results = NULL;
    if (my_count > 0) {
        local_results = malloc(my_count * sizeof(CityStats));
        if (!local_results) {
            fprintf(stderr, "Rank %d: malloc failed\n", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    for (int i = 0; i < my_count; i++) {
        int file_idx = my_file_indices[i];
        strncpy(local_results[i].name, city_names[file_idx], MAX_NAME);
        process_city_file(file_paths[file_idx], &local_results[i]);
    }

    free(my_file_indices);

    // Gather results to rank 0
    // First gather counts
    int* all_counts = NULL;
    int* displacements = NULL;
    if (rank == 0) {
        all_counts = malloc(size * sizeof(int));
        displacements = malloc(size * sizeof(int));
        if (!all_counts || !displacements) {
            fprintf(stderr, "Rank 0: malloc failed for gather arrays\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    MPI_Gather(&my_count, 1, MPI_INT, all_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Create MPI datatype for CityStats (now includes monthly arrays)
    MPI_Datatype city_type;
    int blocklengths[] = {MAX_NAME, 1, 1, 1, 1, 1, 1, 1, 12, 12};
    MPI_Aint offsets[10];
    offsets[0] = offsetof(CityStats, name);
    offsets[1] = offsetof(CityStats, temp_sum);
    offsets[2] = offsetof(CityStats, temp_min);
    offsets[3] = offsetof(CityStats, temp_max);
    offsets[4] = offsetof(CityStats, precip_sum);
    offsets[5] = offsetof(CityStats, temp_count);
    offsets[6] = offsetof(CityStats, precip_count);
    offsets[7] = offsetof(CityStats, record_count);
    offsets[8] = offsetof(CityStats, monthly_temp_sum);
    offsets[9] = offsetof(CityStats, monthly_temp_count);
    MPI_Datatype types[] = {MPI_CHAR, MPI_DOUBLE, MPI_DOUBLE, MPI_DOUBLE, MPI_DOUBLE,
                            MPI_INT, MPI_INT, MPI_INT, MPI_DOUBLE, MPI_INT};

    MPI_Type_create_struct(10, blocklengths, offsets, types, &city_type);
    MPI_Type_commit(&city_type);

    CityStats* all_results = NULL;
    int total_cities = 0;

    if (rank == 0) {
        for (int i = 0; i < size; i++) {
            total_cities += all_counts[i];
        }
        all_results = malloc(total_cities * sizeof(CityStats));
        if (!all_results) {
            fprintf(stderr, "Rank 0: malloc failed for all_results\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        displacements[0] = 0;
        for (int i = 1; i < size; i++) {
            displacements[i] = displacements[i-1] + all_counts[i-1];
        }
    }

    if (strcmp(comm_mode, "nonblocking") == 0) {
        // Non-blocking gather
        MPI_Request request;
        MPI_Igatherv(local_results, my_count, city_type,
                     all_results, all_counts, displacements, city_type,
                     0, MPI_COMM_WORLD, &request);
        MPI_Wait(&request, MPI_STATUS_IGNORE);
    } else {
        // Blocking gather
        MPI_Gatherv(local_results, my_count, city_type,
                    all_results, all_counts, displacements, city_type,
                    0, MPI_COMM_WORLD);
    }

    double end_time = MPI_Wtime();
    double elapsed = end_time - start_time;

    // Get max time across all processes
    double max_elapsed;
    MPI_Reduce(&elapsed, &max_elapsed, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        print_results(all_results, total_cities);

        printf("\n========== PERFORMANCE ==========\n");
        printf("Processing time: %.3f seconds\n", max_elapsed);
        printf("Cities processed: %d\n", total_cities);
        printf("Processes used: %d\n", size);
        printf("Throughput: %.2f cities/second\n", total_cities / max_elapsed);

        free(all_results);
        free(all_counts);
        free(displacements);
    }

    if (local_results) free(local_results);
    MPI_Type_free(&city_type);
    MPI_Finalize();

    return 0;
}
