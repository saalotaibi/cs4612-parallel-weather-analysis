#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <float.h>
#include <sys/time.h>

#define MAX_CITIES 2000
#define MAX_LINE 1024
#define MAX_NAME 128

typedef struct {
    char name[MAX_NAME];
    double temp_sum;
    double temp_min;
    double temp_max;
    double precip_sum;
    int temp_count;
    int precip_count;
    int record_count;
    // Monthly averages (0-11)
    double monthly_temp_sum[12];
    int monthly_temp_count[12];
} CityStats;

static CityStats cities[MAX_CITIES];
static int city_count = 0;

double get_time_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Parse CSV field - copies to buffer to avoid static buffer issues
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

// Extract month from date (YYYY-MM-DD format)
int get_month(const char* date) {
    if (strlen(date) < 7) return -1;
    char month_str[3] = {date[5], date[6], '\0'};
    return atoi(month_str) - 1;  // 0-indexed
}

void process_city_file(const char* filepath, const char* city_name) {
    FILE* fp = fopen(filepath, "r");
    if (!fp) return;

    // Initialize city stats
    CityStats* city = &cities[city_count];
    strncpy(city->name, city_name, MAX_NAME - 1);
    city->name[MAX_NAME - 1] = '\0';
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

        // Fields: station_id(0), city_name(1), date(2), season(3),
        //         avg_temp_c(4), min_temp_c(5), max_temp_c(6),
        //         precipitation_mm(7), ...

        // Get date for month extraction
        char date[32];
        get_field(line, 2, date, sizeof(date));
        int month = get_month(date);

        // Get average temperature
        get_field(line, 4, field_buf, sizeof(field_buf));
        if (field_buf[0] != '\0') {
            double temp = atof(field_buf);
            city->temp_sum += temp;
            city->temp_count++;

            if (temp < city->temp_min) city->temp_min = temp;
            if (temp > city->temp_max) city->temp_max = temp;

            if (month >= 0 && month < 12) {
                city->monthly_temp_sum[month] += temp;
                city->monthly_temp_count[month]++;
            }
        }

        // Get precipitation
        get_field(line, 7, field_buf, sizeof(field_buf));
        if (field_buf[0] != '\0') {
            double precip = atof(field_buf);
            city->precip_sum += precip;
            city->precip_count++;
        }
    }

    fclose(fp);
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

    printf("Weather Analysis - Serial Version\n");
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

        // Extract city name (remove .csv extension)
        char city_name[MAX_NAME];
        strncpy(city_name, entry->d_name, MAX_NAME - 1);
        city_name[MAX_NAME - 1] = '\0';
        char* dot = strrchr(city_name, '.');
        if (dot) *dot = '\0';

        // Replace underscores with spaces
        for (char* p = city_name; *p; p++) {
            if (*p == '_') *p = ' ';
        }

        process_city_file(filepath, city_name);
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
