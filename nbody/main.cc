#include <omp.h>
#include <cstdio>
#include <cmath>
#include <thread>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>
#include <thread>
#include <pthread.h>
#include <complex>
#include <cstring>
#include <thread>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/time.h>

using namespace std;

ibv_device **curlist;
Worker *worker[10];
Master *master;
int num_worker = 0;
WorkerHandle *malloc_wh;
WorkerHandle *wh[10];

int iteration_times = 1;
int no_run = 3;
int parrallel_num = 4;

const double GRAVITATIONAL_CONSTANT = 6.6726e-11; /* N(m/kg)2 */

const double DEFAULT_DOMAIN_SIZE_X = 1.0e+18; /* m  */
const double DEFAULT_DOMAIN_SIZE_Y = 1.0e+18; /* m  */
const double DEFAULT_DOMAIN_SIZE_Z = 1.0e+18; /* m  */
const double DEFAULT_MASS_MAXIMUM = 1.0e+18;  /* kg */
const double DEFAULT_TIME_INTERVAL = 1.0e+18; /* s  */
const int DEFAULT_NUMBER_OF_PARTICLES = 1000;
const int DEFAULT_NUMBER_OF_TIMESTEPS = 100;
const int DEFAULT_TIMESTEPS_BETWEEN_OUTPUTS = DEFAULT_NUMBER_OF_PARTICLES;
const bool DEFAULT_EXECUTE_SERIAL = false;
const int DEFAULT_RANDOM_SEED = 12345;

const int DEFAULT_STRING_LENGTH = 1023;

const int PROGRAM_SUCCESS_CODE = 0;

/*
 * Global variables - input
 */
char base_filename[DEFAULT_STRING_LENGTH + 1];

int number_of_particles;
int block_size;
float domain_size_x;
float domain_size_y;
float domain_size_z;
float time_interval;
int number_of_timesteps;
int timesteps_between_outputs;
bool execute_serial;
unsigned random_seed;
float mass_maximum;
// #define CHECK
// #define CHECK_VAL
/*
 * Compute variables
 */
GAddr particle_array_msiInput;
GAddr particle_array_rcInput;
GAddr particle_array_msiOutput;
GAddr particle_array_rcOutput;

/*
 * Types
 */
struct Particle
{
    float position_x; /* m   */
    float position_y; /* m   */
    float position_z; /* m   */
    float velocity_x; /* m/s */
    float velocity_y; /* m/s */
    float velocity_z; /* m/s */
    float total_force_x;
    float total_force_y;
    float total_force_z;
    float mass; /* kg  */
    // float pad;
};

/*
 * Function Prototypes
 */
void Particle_input_arguments(FILE *input);

// Particle
void Particle_clear(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_construct(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_destruct(WorkerHandle *Cur_wh, GAddr this_particle, int index);

// Particle array
GAddr Particle_array_allocate(WorkerHandle *Cur_wh, int number_of_particles);
GAddr Particle_array_construct(WorkerHandle *Cur_wh, int number_of_particles);
GAddr Particle_array_deallocate(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles);
GAddr Particle_array_destruct(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles);

void Particle_set_position_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_array_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles);
void Particle_array_initialize(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles);

void Particle_array_output(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles);
void Particle_array_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles);
void Particle_output(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle, int index);

// Check
#ifdef CHECK
void Particle_check(GAddr this_particle, char *action, char *routine);
void Particle_array_check(GAddr this_particle_array, int number_of_particles,
                          char *action, char *routine);
#endif

/* wall_time */
long wtime();

void Create_master()
{
    Conf *conf = new Conf();
    // conf->loglevel = LOG_DEBUG;
    conf->loglevel = LOG_TEST;
    GAllocFactory::SetConf(conf);
    master = new Master(*conf);
}

void Create_worker()
{
    Conf *conf = new Conf();
    RdmaResource *res = new RdmaResource(curlist[0], false);
    conf->worker_port += num_worker;
    worker[num_worker] = new Worker(*conf, res);
    wh[num_worker] = new WorkerHandle(worker[num_worker]);
    num_worker++;
}

void Read_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size)
{
    WorkRequest wr{};
    wr.op = READ;
    wr.wid = Cur_wh->GetWorkerId();
    wr.flag = 0;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Write_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size, int flush_id)
{
    WorkRequest wr{};
    if (Cur_wh->GetWorkerId() == 0)
    {
        flush_id = -1;
    }

    wr.Reset();
    wr.op = WRITE;
    wr.wid = Cur_wh->GetWorkerId();
    // wr.flag = ASYNC; // 可以在这里调
    wr.flush_id = flush_id;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

GAddr Malloc_addr(WorkerHandle *Cur_wh, const Size size, Flag flag, int Owner)
{
#ifdef LOCAL_MEMORY_HOOK
    void *laddr = zmalloc(size);
    return (GAddr)laddr;
#else
    WorkRequest wr = {};
    wr.op = MALLOC;
    wr.flag = flag;
    wr.size = size;
    wr.arg = Owner;

    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "malloc failed");
        return Gnullptr;
    }
    else
    {
        epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
        return wr.addr;
    }
#endif
}

void Free_addr(WorkerHandle *Cur_wh, GAddr addr)
{
    WorkRequest wr = {};
    wr.Reset();
    wr.addr = addr;
    wr.op = FREE;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void calculate_force_addr(WorkerHandle *Cur_wh, GAddr this_particle1, GAddr this_particle2, GAddr this_particle3,
                          float *force_x, float *force_y, float *force_z)
{
    /* Particle calculate force */
    float difference_x, difference_y, difference_z;
    float distance_squared, distance;
    float force_magnitude;

    Particle p1, p2, p3;
    Read_val(Cur_wh, this_particle1, &p1, sizeof(Particle));
    Read_val(Cur_wh, this_particle2, &p2, sizeof(Particle));

    difference_x = p2.position_x - p1.position_x;
    difference_y = p2.position_y - p1.position_y;
    difference_z = p2.position_z - p1.position_z;

    distance_squared = difference_x * difference_x +
                       difference_y * difference_y +
                       difference_z * difference_z;

    distance = std::sqrt(distance_squared); // sqrtf(distance_squared);

    force_magnitude = GRAVITATIONAL_CONSTANT * (p1.mass) * (p2.mass) / distance_squared;

    *force_x = (force_magnitude / distance) * difference_x;
    *force_y = (force_magnitude / distance) * difference_y;
    *force_z = (force_magnitude / distance) * difference_z;

    Write_val(Cur_wh, this_particle3, &p3, sizeof(Particle), 1);
}
void calculate_force(WorkerHandle *Cur_wh, Particle *this_particle1, Particle *this_particle2,
                     float *force_x, float *force_y, float *force_z)
{
    /* Particle calculate force */
    float difference_x, difference_y, difference_z;
    float distance_squared, distance;
    float force_magnitude;

    Particle p1 = *this_particle1;
    Particle p2 = *this_particle2;

    difference_x = p2.position_x - p1.position_x;
    difference_y = p2.position_y - p1.position_y;
    difference_z = p2.position_z - p1.position_z;

    distance_squared = difference_x * difference_x +
                       difference_y * difference_y +
                       difference_z * difference_z;

    distance = std::sqrt(distance_squared); // sqrtf(distance_squared);

    force_magnitude = GRAVITATIONAL_CONSTANT * (p1.mass) * (p2.mass) / distance_squared;

    *force_x = (force_magnitude / distance) * difference_x;
    *force_y = (force_magnitude / distance) * difference_y;
    *force_z = (force_magnitude / distance) * difference_z;
}

void sub_nbody(WorkerHandle *Cur_wh, GAddr first_particles, GAddr second_particles, int index)
{
    GAddr first_particles_index = first_particles + index * sizeof(Particle);
    GAddr second_particles_index = second_particles + index * sizeof(Particle);
    float force_x = 0.0f, force_y = 0.0f, force_z = 0.0f;
    float total_force_x = 0.0f, total_force_y = 0.0f, total_force_z = 0.0f;

    int i;
    Particle first_particles_array[number_of_particles];
    Read_val(Cur_wh, first_particles, first_particles_array, sizeof(Particle) * number_of_particles);

    for (i = 0; i < number_of_particles; i++)
    {
        if (i != index)
        {
            // calculate_force(WorkerHandle *Cur_wh, Particle *this_particle1, Particle *this_particle2,float *force_x, float *force_y, float *force_z)
            calculate_force(Cur_wh, &first_particles_array[index], &first_particles_array[i], &force_x, &force_y, &force_z);

            // Read_val(Cur_wh, second_particles_index + sizeof(float) * 6, &total_force_x, sizeof(float));
            total_force_x += force_x;
            // Write_val(Cur_wh, second_particles_index + sizeof(float) * 6, &total_force_x, sizeof(float), 1);

            // Read_val(Cur_wh, second_particles_index + sizeof(float) * 7, &total_force_y, sizeof(float));
            total_force_y += force_y;
            // Write_val(Cur_wh, second_particles_index + sizeof(float) * 7, &total_force_y, sizeof(float), 1);

            // Read_val(Cur_wh, second_particles_index + sizeof(float) * 8, &total_force_z, sizeof(float));
            total_force_z += force_z;
            // Write_val(Cur_wh, second_particles_index + sizeof(float) * 8, &total_force_z, sizeof(float), 1);
        }
    }

    float velocity_change_x, velocity_change_y, velocity_change_z;
    float position_change_x, position_change_y, position_change_z;

    Particle first_particles_buf = first_particles_array[index];
    Particle second_particles_buf;
    // Read_val(Cur_wh, first_particles_index, &first_particles_buf, sizeof(Particle));
    // Read_val(Cur_wh, second_particles_index, &second_particles_buf, sizeof(Particle));

    second_particles_buf.mass = first_particles_buf.mass;

    velocity_change_x = total_force_x * (time_interval / first_particles_buf.mass);
    velocity_change_y = total_force_y * (time_interval / first_particles_buf.mass);
    velocity_change_z = total_force_z * (time_interval / first_particles_buf.mass);

    position_change_x = first_particles_buf.velocity_x + velocity_change_x * (0.5 * time_interval);
    position_change_y = first_particles_buf.velocity_y + velocity_change_y * (0.5 * time_interval);
    position_change_z = first_particles_buf.velocity_z + velocity_change_z * (0.5 * time_interval);

    second_particles_buf.velocity_x = first_particles_buf.velocity_x + velocity_change_x;
    second_particles_buf.velocity_y = first_particles_buf.velocity_y + velocity_change_y;
    second_particles_buf.velocity_z = first_particles_buf.velocity_z + velocity_change_z;

    second_particles_buf.position_x = first_particles_buf.position_x + position_change_x;
    second_particles_buf.position_y = first_particles_buf.position_y + position_change_y;
    second_particles_buf.position_z = first_particles_buf.position_z + position_change_z;

    Write_val(Cur_wh, second_particles_index, &second_particles_buf, sizeof(Particle), 1);
    // struct Particle
    // {
    //     float position_x; /* m   */
    //     float position_y; /* m   */
    //     float position_z; /* m   */
    //     float velocity_x; /* m/s */
    //     float velocity_y; /* m/s */
    //     float velocity_z; /* m/s */
    //     float total_force_x;
    //     float total_force_y;
    //     float total_force_z;
    //     float mass; /* kg  */
    //     // float pad;
    // };
    // Write_val(Cur_wh, second_particles_index, &second_particles_buf.position_x, sizeof(float), 1);
    // Write_val(Cur_wh, second_particles_index + sizeof(float), &second_particles_buf.position_y, sizeof(float), 1);
    // Write_val(Cur_wh, second_particles_index + sizeof(float) * 2, &second_particles_buf.position_z, sizeof(float), 1);
    // Write_val(Cur_wh, second_particles_index + sizeof(float) * 3, &second_particles_buf.velocity_x, sizeof(float), 1);
    // Write_val(Cur_wh, second_particles_index + sizeof(float) * 4, &second_particles_buf.velocity_y, sizeof(float), 1);
    // Write_val(Cur_wh, second_particles_index + sizeof(float) * 5, &second_particles_buf.velocity_z, sizeof(float), 1);
    // Write_val(Cur_wh, second_particles_index + sizeof(float) * 9, &second_particles_buf.mass, sizeof(float), 1);
}

void nbody(GAddr first_particles, GAddr second_particles)
{

    std::thread threads[parrallel_num];

    for (int id = 0; id < number_of_particles; id++)
    {
        int thread_id = id % parrallel_num;
        threads[thread_id] = std::thread(sub_nbody, wh[thread_id], first_particles, second_particles, id);
        threads[thread_id].join();
    }

#ifdef CHECK_VAL
    for (int i = 0; i < parrallel_num; i++)
    {
        wh[i]->ReportCacheStatistics();
    }
#endif
}

void nbody_serial(GAddr first_particles, GAddr second_particles)
{
    for (int id = 0; id < number_of_particles; id++)
    {
        sub_nbody(wh[0], first_particles, second_particles, id);
    }
}

void nbody_rc(GAddr first_particles, GAddr second_particles)
{
    for (int i = 0; i < parrallel_num; i++)
    {
        // acquireLock(int id, GAddr addr, int size, bool flag, int flush_size)
        wh[i]->acquireLock(1, second_particles, sizeof(Particle) * number_of_particles, true, sizeof(Particle));
        // wh[i]->acquireLock(1, second_particles, sizeof(Particle) * number_of_particles, false, sizeof(float));
    }
    std::thread threads[parrallel_num];

    for (int id = 0; id < number_of_particles; id++)
    {
        int thread_id = id % parrallel_num;
        threads[thread_id] = std::thread(sub_nbody, wh[thread_id], first_particles, second_particles, id);
        threads[thread_id].join();
    }
    for (int i = 0; i < parrallel_num; i++)
    {
        wh[i]->releaseLock(1, second_particles);
    }
}

void particle_print(WorkerHandle *Cur_wh, GAddr particle, int number_of_particles)
{
    printf("\n\nPrinting %d particles:\n", number_of_particles);
    for (int i = 0; i < 3; i++)
    {
        Particle particle_buf;
        Read_val(Cur_wh, particle + i * sizeof(Particle), &particle_buf, sizeof(Particle));
        printf("Particle %d:\n", i);
        printf("Mass: %f\n", particle_buf.mass);
        printf("Position: (%f, %f, %f)\n", particle_buf.position_x, particle_buf.position_y, particle_buf.position_z);
        printf("Velocity: (%f, %f, %f)\n", particle_buf.velocity_x, particle_buf.velocity_y, particle_buf.velocity_z);
    }
}

void Solve_MSI()
{

    particle_array_msiOutput = Malloc_addr(wh[0], sizeof(Particle) * number_of_particles, Msi, 1);

    printf("particle_array_msiOutput: %lx\n", particle_array_msiOutput);

    printf("Begin N-body simulation MSI\n");

    long start = wtime();

    for (int timestep = 1; timestep <= number_of_timesteps; timestep++)
    {
        nbody(particle_array_msiInput, particle_array_msiOutput);

        particle_print(wh[0], particle_array_msiOutput, number_of_particles);

        /* swap arrays */

        Particle tmp1[number_of_particles], tmp2[number_of_particles];

        Read_val(wh[0], particle_array_msiInput, tmp1, sizeof(Particle) * number_of_particles);
        Read_val(wh[0], particle_array_msiOutput, tmp2, sizeof(Particle) * number_of_particles);
        Write_val(wh[0], particle_array_msiInput, tmp2, sizeof(Particle) * number_of_particles, 1);
        Write_val(wh[0], particle_array_msiOutput, tmp1, sizeof(Particle) * number_of_particles, 1);

        printf("Iteration %d complete\n", timestep);
    }

    long end = wtime();
    double time = (end - start) / 1000000.0;

    printf("NBody simulation complete\n");
    printf("Number of Particles: %d\n", number_of_particles);
    printf("Number of Iterations: %d\n", number_of_timesteps);
    printf("Run time: %.3f s\n", time);

#ifdef VERBOSE
#endif

    // particle_array_msiInput = Particle_array_destruct(wh[0], particle_array_msiInput, number_of_particles);
    // particle_array_msiOutput = Particle_array_destruct(wh[0], particle_array_msiOutput, number_of_particles);
}

void Solve_RC()
{
    particle_array_rcOutput = Malloc_addr(wh[0], sizeof(Particle) * number_of_particles, RC_Write_shared, 1);

    printf("particle_array_rcOutput: %lx\n", particle_array_rcOutput);

    printf("Begin N-body simulation RC\n");

    long start = wtime();

    for (int timestep = 1; timestep <= number_of_timesteps; timestep++)
    {
        nbody_rc(particle_array_rcInput, particle_array_rcOutput);

        particle_print(wh[0], particle_array_rcOutput, number_of_particles);

        /* swap arrays */

        Particle tmp1[number_of_particles], tmp2[number_of_particles];
        Read_val(wh[0], particle_array_rcInput, tmp1, sizeof(Particle) * number_of_particles);
        Read_val(wh[0], particle_array_rcOutput, tmp2, sizeof(Particle) * number_of_particles);
        Write_val(wh[0], particle_array_rcInput, tmp2, sizeof(Particle) * number_of_particles, 1);
        Write_val(wh[0], particle_array_rcOutput, tmp1, sizeof(Particle) * number_of_particles, 1);

        printf("Iteration %d complete\n", timestep);
    }

    long end = wtime();
    double time = (end - start) / 1000000.0;

    printf("NBody simulation complete\n");
    printf("Number of Particles: %d\n", number_of_particles);
    printf("Number of Iterations: %d\n", number_of_timesteps);
    printf("Run time: %.3f s\n", time);

#ifdef VERBOSE
#endif

    // particle_array_msiInput = Particle_array_destruct(wh[0], particle_array_msiInput, number_of_particles);
    // particle_array_msiOutput = Particle_array_destruct(wh[0], particle_array_msiOutput, number_of_particles);
}

/*
 * Get command line arguments.
 */
void Particle_input_arguments(FILE *input)
{
    number_of_particles = DEFAULT_NUMBER_OF_PARTICLES;
    block_size = DEFAULT_NUMBER_OF_PARTICLES;
    domain_size_x = DEFAULT_DOMAIN_SIZE_X;
    domain_size_y = DEFAULT_DOMAIN_SIZE_Y;
    domain_size_z = DEFAULT_DOMAIN_SIZE_Z;
    time_interval = DEFAULT_TIME_INTERVAL;
    number_of_timesteps = DEFAULT_NUMBER_OF_PARTICLES;
    timesteps_between_outputs = DEFAULT_TIMESTEPS_BETWEEN_OUTPUTS;
    execute_serial = DEFAULT_EXECUTE_SERIAL;
    random_seed = DEFAULT_RANDOM_SEED;
    mass_maximum = DEFAULT_MASS_MAXIMUM;

    if (fscanf(input, "%d", &number_of_particles) != 1)
    {
        fprintf(stderr, "ERROR: cannot read number of particles from standard input!\n");
        std::abort();
    }

    if (number_of_particles < 1)
    {
        fprintf(stderr, "ERROR: cannot have %d particles!\n", number_of_particles);
        std::abort();
    }

    if (number_of_particles == 1)
    {
        fprintf(stderr, "There is only one particle, therefore no forces.\n");
        std::abort();
    }
    //
    if (fscanf(input, "%d", &block_size) != 1)
    {
        fprintf(stderr, "ERROR: cannot read block size from standard input!\n");
        std::abort();
    }

    if (block_size <= 0)
    {
        fprintf(stderr, "ERROR: cannot have %d as block size!\n", block_size);
        std::abort();
    }

    if (number_of_particles % block_size != 0)
    {
        fprintf(stderr, "ERROR: block size must be divisable by number of particles!\n");
        std::abort();
    }

    if (fscanf(input, "%f", &domain_size_x) != 1)
    {
        fprintf(stderr, "ERROR: cannot read domain size X from standard input!\n");
        std::abort();
    }

    if (domain_size_x <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a domain whose X dimension has length %f!\n", domain_size_x);
        std::abort();
    }

    if (fscanf(input, "%f", &domain_size_y) != 1)
    {
        fprintf(stderr, "ERROR: cannot read domain size Y from standard input!\n");
        std::abort();
    }

    if (domain_size_y <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a domain whose Y dimension has length %f!\n", domain_size_y);
        std::abort();
    }

    if (fscanf(input, "%f", &domain_size_z) != 1)
    {
        fprintf(stderr, "ERROR: cannot read domain size Z from standard input!\n");
        std::abort();
    }

    if (domain_size_z <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a domain whose Z dimension has length %f!\n", domain_size_z);
        std::abort();
    }

    if (fscanf(input, "%f", &time_interval) != 1)
    {
        fprintf(stderr, "ERROR: cannot read time interval from standard input!\n");
        std::abort();
    }

    if (time_interval <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a time interval of %f!\n", time_interval);
        std::abort();
    }

    if (fscanf(input, "%d", &number_of_timesteps) != 1)
    {
        fprintf(stderr, "ERROR: cannot read number of timesteps from standard input!\n");
        std::abort();
    }

    if (number_of_timesteps <= 0)
    {
        fprintf(stderr, "ERROR: cannot have %d timesteps!\n", number_of_timesteps);
        std::abort();
    }

    if (fscanf(input, "%d", &timesteps_between_outputs) != 1)
    {
        fprintf(stderr, "ERROR: cannot read timesteps between outputs from standard input!\n");
        std::abort();
    }

    if (timesteps_between_outputs <= 0)
    {
        fprintf(stderr, "ERROR: cannot have %d timesteps between outputs!\n", timesteps_between_outputs);
        std::abort();
    }

    int aux_serial;
    if (fscanf(input, "%d", &aux_serial) != 1)
    {
        fprintf(stderr, "ERROR: cannot read serial from standard input!\n");
        std::abort();
    }

    if (aux_serial != 0 && aux_serial != 1)
    {
        fprintf(stderr, "ERROR: serial must be 0 (false) or 1 (true)!\n");
        std::abort();
    }
#ifdef VERBOSE
    execute_serial = (aux_serial == 0) ? false : true;
#endif

    if (fscanf(input, "%d", &random_seed) != 1)
    {
        fprintf(stderr, "ERROR: cannot read random seed from standard input!\n");
        std::abort();
    }

    if (fscanf(input, "%f", &mass_maximum) != 1)
    {
        fprintf(stderr, "ERROR: cannot read mass maximum from standard input!\n");
        std::abort();
    }

    if (mass_maximum <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a maximum mass of %f!\n", mass_maximum);
        std::abort();
    }

    fgetc(input);
    fgets(base_filename, DEFAULT_STRING_LENGTH, input);
    if (base_filename[strlen(base_filename) - 1] == '\n')
    {
        base_filename[strlen(base_filename) - 1] = '\0';
    }
}

/*
 * Clear the particle's data.
 */
void Particle_clear(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "clear", "Particle clear");
#endif
    Particle particle_init;

    particle_init.position_x = 0.0;
    particle_init.position_y = 0.0;
    particle_init.position_z = 0.0;
    particle_init.velocity_x = 0.0;
    particle_init.velocity_y = 0.0;
    particle_init.velocity_z = 0.0;
    particle_init.mass = 0.0;
    Write_val(Cur_wh, this_particle + index * sizeof(Particle), &particle_init, sizeof(Particle), 1);
}

/*
 * Construct the particle.
 */
void Particle_construct(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "construct", "Particle construct");
#endif

    Particle_clear(Cur_wh, this_particle, index);
}

/*
 * Destroy the particle.
 */
void Particle_destruct(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "destruct", "Particle_destruct");
#endif

    Particle_clear(Cur_wh, this_particle, index);
}

/*
 * Initialize the particle by setting its data randomly.
 */
void Particle_set_position_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "randomly set the position", "Particle_set_randomly");
#endif

    Particle particle_init;
    particle_init.position_x = domain_size_x * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
    particle_init.position_y = domain_size_y * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
    particle_init.position_z = domain_size_z * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
    particle_init.velocity_x = 0.0;
    particle_init.velocity_y = 0.0;
    particle_init.velocity_z = 0.0;
    particle_init.mass = mass_maximum * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
    Write_val(Cur_wh, this_particle + index * sizeof(Particle), &particle_init, sizeof(Particle), 1);
}

/*
 * Initialize the particle by setting its data randomly.
 */
void Particle_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "randomly initialize", "Particle initialize randomly");
#endif

    // Particle_clear(Cur_wh, this_particle, index);
    Particle_set_position_randomly(Cur_wh, this_particle, index);

#ifdef CHECK_VAL
    Particle particle_buf;
    Read_val(Cur_wh, this_particle + index * sizeof(Particle), &particle_buf, sizeof(Particle));
    printf("mass %g\n", particle_buf.mass);
#endif
}

void Particle_output(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
    Particle particle;
    Read_val(Cur_wh, this_particle + index * sizeof(Particle), &particle, sizeof(Particle));

    printf("%g %g %g %g %g %g %g\n",
           particle.position_x,
           particle.position_y,
           particle.position_z,
           particle.velocity_x,
           particle.velocity_y,
           particle.velocity_z,
           particle.mass);
}

void Particle_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
    Particle particle;
    Read_val(Cur_wh, this_particle + index * sizeof(Particle), &particle, sizeof(Particle));
    printf("C %g %g %g\n",
           particle.position_x, particle.position_y, particle.position_z);
}

/*
 * Allocate and return an array of particles.
 */
GAddr Particle_array_allocate(WorkerHandle *Cur_wh, int number_of_particles)
{
    GAddr this_particle_array;

#ifdef CHECK
    if (number_of_particles < 0)
    {
        fprintf(stderr, "ERROR: illegal number of particles %d to allocate\n", number_of_particles);
        fprintf(stderr, "  in Particle array construct\n");
        std::abort();
    }
#endif

    if (number_of_particles == 0)
        return -1;

    this_particle_array = Malloc_addr(Cur_wh, number_of_particles * sizeof(Particle), Msi, 1);
    if (this_particle_array == -1)
    {
        fprintf(stderr, "ERROR: can't allocate a particle array of %d particles\n", number_of_particles);
        fprintf(stderr, "  in Particle array construct\n");
        std::abort();
    }

    return this_particle_array;
}

/*
 * Construct and return an array of particles, cleared.
 */
GAddr Particle_array_construct(WorkerHandle *Cur_wh, int number_of_particles)
{
    GAddr this_particle_array;

    this_particle_array = Particle_array_allocate(Cur_wh, number_of_particles);

    for (int index = 0; index < number_of_particles; index++)
    {
        Particle_construct(Cur_wh, this_particle_array, index);
    }
    return this_particle_array;
}

/*
 * Deallocate the array of particles, and return NULL.
 */
GAddr Particle_array_deallocate(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles)
{
#ifdef CHECK
    Particle_array_check(this_particle_array, number_of_particles, "deallocate", "Particle_array_deallocate");
#endif

    Free_addr(Cur_wh, this_particle_array);

    return 0;
}

/*
 * Destroy the array of particles, and return NULL.
 */
GAddr Particle_array_destruct(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles)
{
#ifdef CHECK
    Particle_array_check(this_particle_array, number_of_particles, "destroy", "Particle array destruct");
#endif

    for (int index = number_of_particles - 1; index >= 0; index--)
    {
        Particle_destruct(Cur_wh, this_particle_array, index);
    }

    return Particle_array_deallocate(Cur_wh, this_particle_array, number_of_particles);
}

/*
 * Initialize the array of particles by setting its data randomly.
 */
void Particle_array_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles)
{
#ifdef CHECK
    Particle_array_check(this_particle_array, number_of_particles,
                         "initialize randomly", "Particle_array_initialize_randomly");
#endif

    for (int index = 0; index < number_of_particles; index++)
    {
        Particle_initialize_randomly(Cur_wh, this_particle_array, index);
    }
}

/*
 * Initialize the array of particles.
 */
void Particle_array_initialize(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles)
{
    Particle_array_initialize_randomly(Cur_wh, this_particle_array, number_of_particles);
}

/*
 * Particle_array_output
 */
void Particle_array_output(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles)
{
    printf("%d\nNBody\n", number_of_particles);

    for (int index = 0; index < number_of_particles; index++)
    {
        Particle_output(Cur_wh, this_particle_array, index);
    }
} /* Particle_array_output */

/* Outputs particle positions in a format that VMD can easily visualize. */
void Particle_array_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle_array, int number_of_particles)
{
    printf("%d\nNBody\n", number_of_particles);

    for (int index = 0; index < number_of_particles; index++)
    {
        Particle_output_xyz(Cur_wh, this_particle_array, index);
    }
}

#ifdef CHECK
/*
 * Check that the particle exists.
 */
void Particle_check(GAddr this_particle, char *action, char *routine)
{
    if (this_particle != 0)
        return;

    printf("ERROR: can't %s a nonexistent particle\n",
           ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
    printf("  in %s\n",
           ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

    std::abort();
}

void Particle_array_check(GAddr this_particle_array, int number_of_particles,
                          char *action, char *routine)
{
    if (number_of_particles < 0)
    {
        printf("ERROR: illegal number of particles %d\n", number_of_particles);
        printf("  to %s\n",
               ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
        printf("  in %s\n",
               ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

        std::abort();
    }

    if (number_of_particles == 0)
    {
        printf("ERROR: can't %s\n",
               ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
        printf("  an existing particle array of length 0\n");
        printf("  in %s\n",
               ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

        std::abort();
    }

    if (this_particle_array == 0)
    {
        printf("ERROR: can't %s\n",
               ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
        printf("  a nonexistent array of %d particles\n", number_of_particles);
        printf("  in %s\n",
               ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

        std::abort();
    }
    return;
}
#endif /* #ifdef CHECK */

/* wall_time */
long wtime()
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec * 1000000 + t.tv_usec;
}

/* main */
int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cout << "Informe um arquivo com os parâmetros de entrada: ./nbody_simulation <input_file.in>\n";
        std::abort();
    }
    FILE *input_data = fopen(argv[1], "r");
    Particle_input_arguments(input_data);

    srand(time(NULL));
    curlist = ibv_get_device_list(NULL);
    Create_master();

    for (int i = 0; i < parrallel_num; ++i)
        Create_worker();
    if (no_run == 1)
    {
        particle_array_msiInput = Malloc_addr(wh[0], sizeof(Particle) * number_of_particles, Msi, 1);
        Particle_array_initialize(wh[0], particle_array_msiInput, number_of_particles);
        Solve_MSI();
    }
    else if (no_run == 2)
    {
        particle_array_rcInput = Malloc_addr(wh[0], sizeof(Particle) * number_of_particles, Msi, 1);
        Particle_array_initialize(wh[0], particle_array_rcInput, number_of_particles);
        Solve_RC();
    }
    else if (no_run == 3)
    {
        particle_array_msiInput = Malloc_addr(wh[0], sizeof(Particle) * number_of_particles, Msi, 1);
        particle_array_rcInput = Malloc_addr(wh[0], sizeof(Particle) * number_of_particles, Msi, 1);

        Particle_array_initialize(wh[0], particle_array_msiInput, number_of_particles);

        Particle tmp[number_of_particles];
        Read_val(wh[0], particle_array_msiInput, tmp, sizeof(Particle) * number_of_particles);
        Write_val(wh[0], particle_array_rcInput, tmp, sizeof(Particle) * number_of_particles, 1);

        Solve_MSI();
        Solve_RC();
    }

    return PROGRAM_SUCCESS_CODE;
}