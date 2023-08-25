// /*
//     Alunos: Vitor Wachholz de Pinho e Igor Rosler
//     Disciplina: IPPD
// */

// // 1. Criar classe para os corpos
// // 2. Fazer as funções necessarias para os calculos
// // 3. Testar os calculos
// // 4. Adicionar a multiprogramação (OpenMP)
// // 5. Ler os parametros de um arquivo
// // 6. Criar make file
// // 7. Coletar dados para gerar os graficos
// // 8. Fazer uma analise do desempenho
// // 9. Redigir um README bem explicado

// #include <omp.h>
// #include <cstdio>
// #include <cmath>
// #include <thread>
// #include <math.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>
// #include <iostream>
// #include <thread>
// #include <pthread.h>
// #include <complex>
// #include <cstring>
// #include <thread>
// #include "structure.h"
// #include "worker.h"
// #include "settings.h"
// #include "worker_handle.h"
// #include "master.h"
// #include "gallocator.h"
// #include "workrequest.h"
// #include <cstdlib>
// #include <cstdio>
// #include <cstring>
// #include <sys/time.h>

// using namespace std;

// ibv_device **curlist;
// Worker *worker[10];
// Master *master;
// int num_worker = 0;
// WorkerHandle *malloc_wh;
// WorkerHandle *wh[10];

// int iteration_times = 1;
// int no_run = 1;
// int parrallel_num = 2;

// const double GRAVITATIONAL_CONSTANT = 6.6726e-11; /* N(m/kg)2 */

// const double DEFAULT_DOMAIN_SIZE_X = 1.0e+18; /* m  */
// const double DEFAULT_DOMAIN_SIZE_Y = 1.0e+18; /* m  */
// const double DEFAULT_DOMAIN_SIZE_Z = 1.0e+18; /* m  */
// const double DEFAULT_MASS_MAXIMUM = 1.0e+18;  /* kg */
// const double DEFAULT_TIME_INTERVAL = 1.0e+18; /* s  */
// const int DEFAULT_NUMBER_OF_PARTICLES = 1000;
// const int DEFAULT_NUMBER_OF_TIMESTEPS = 100;
// const int DEFAULT_TIMESTEPS_BETWEEN_OUTPUTS = DEFAULT_NUMBER_OF_PARTICLES;
// const bool DEFAULT_EXECUTE_SERIAL = false;
// const int DEFAULT_RANDOM_SEED = 12345;

// const int DEFAULT_STRING_LENGTH = 1023;

// const int PROGRAM_SUCCESS_CODE = 0;

// /*
//  * Global variables - input
//  */
// char base_filename[DEFAULT_STRING_LENGTH + 1];

// int number_of_particles;
// int block_size;
// float domain_size_x;
// float domain_size_y;
// float domain_size_z;
// float time_interval;
// int number_of_timesteps;
// int timesteps_between_outputs;
// bool execute_serial;
// unsigned random_seed;
// float mass_maximum;

// /*
//  * Types
//  */
// struct Particle
// {
//     float position_x; /* m   */
//     float position_y; /* m   */
//     float position_z; /* m   */
//     float velocity_x; /* m/s */
//     float velocity_y; /* m/s */
//     float velocity_z; /* m/s */
//     float mass;       /* kg  */
//     float pad;
// };

// /*
//  * Function Prototypes
//  */
// void Particle_input_arguments(FILE *input);

// // Particle
// void Particle_clear(Particle *this_particle);
// void Particle_construct(Particle *this_particle);
// void Particle_destruct(Particle *this_particle);
// void Particle_set_position_randomly(Particle *this_particle);
// void Particle_initialize_randomly(Particle *this_particle);
// void Particle_initialize(Particle *this_particle);
// void Particle_output(FILE *fileptr, Particle *this_particle);
// void Particle_output_xyz(FILE *fileptr, Particle *this_particle);

// // Particle array
// Particle *Particle_array_allocate(int number_of_particles);
// Particle *Particle_array_construct(int number_of_particles);
// Particle *Particle_array_deallocate(Particle *this_particle_array, int number_of_particles);
// Particle *Particle_array_destruct(Particle *this_particle_array, int number_of_particles);
// void Particle_array_initialize_randomly(Particle *this_particle_array, int number_of_particles);
// void Particle_array_initialize(Particle *this_particle_array, int number_of_particles);
// void Particle_array_output(char *base_filename, Particle *this_particle_array, int number_of_particles, int timestep);
// void Particle_array_output_xyz(FILE *fileptr, Particle *this_particle_array, int number_of_particles);

// // Check
// #ifdef CHECK
// void Particle_check(Particle *this_particle, char *action, char *routine);
// void Particle_array_check(Particle *this_particle_array, int number_of_particles,
//                           char *action, char *routine);
// #endif

// /* wall_time */
// long wtime();

// void Create_master()
// {
//     Conf *conf = new Conf();
//     // conf->loglevel = LOG_DEBUG;
//     conf->loglevel = LOG_TEST;
//     GAllocFactory::SetConf(conf);
//     master = new Master(*conf);
// }

// void Create_worker()
// {
//     Conf *conf = new Conf();
//     RdmaResource *res = new RdmaResource(curlist[0], false);
//     conf->worker_port += num_worker;
//     worker[num_worker] = new Worker(*conf, res);
//     wh[num_worker] = new WorkerHandle(worker[num_worker]);
//     num_worker++;
// }

// void Read_val(WorkerHandle *Cur_wh, GAddr addr, int *val, int size)
// {
//     WorkRequest wr{};
//     wr.op = READ;
//     wr.wid = Cur_wh->GetWorkerId();
//     wr.flag = 0;
//     wr.size = size;
//     wr.addr = addr;
//     wr.ptr = (void *)val;
//     if (Cur_wh->SendRequest(&wr))
//     {
//         epicLog(LOG_WARNING, "send request failed");
//     }
// }

// void Write_val(WorkerHandle *Cur_wh, GAddr addr, int *val, int size, int flush_id)
// {
//     WorkRequest wr{};
//     if (Cur_wh->GetWorkerId() == 0)
//     {
//         flush_id = -1;
//     }

//     wr.Reset();
//     wr.op = WRITE;
//     wr.wid = Cur_wh->GetWorkerId();
//     // wr.flag = ASYNC; // 可以在这里调
//     wr.flush_id = flush_id;
//     wr.size = size;
//     wr.addr = addr;
//     wr.ptr = (void *)val;
//     if (Cur_wh->SendRequest(&wr))
//     {
//         epicLog(LOG_WARNING, "send request failed");
//     }
// }

// GAddr Malloc_addr(WorkerHandle *Cur_wh, const Size size, Flag flag, int Owner)
// {
// #ifdef LOCAL_MEMORY_HOOK
//     void *laddr = zmalloc(size);
//     return (GAddr)laddr;
// #else
//     WorkRequest wr = {};
//     wr.op = MALLOC;
//     wr.flag = flag;
//     wr.size = size;
//     wr.arg = Owner;

//     if (Cur_wh->SendRequest(&wr))
//     {
//         epicLog(LOG_WARNING, "malloc failed");
//         return Gnullptr;
//     }
//     else
//     {
//         epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
//         return wr.addr;
//     }
// #endif
// }

// void Free_addr(WorkerHandle *Cur_wh, GAddr addr)
// {
//     WorkRequest wr = {};
//     wr.Reset();
//     wr.addr = addr;
//     wr.op = FREE;
//     if (Cur_wh->SendRequest(&wr))
//     {
//         epicLog(LOG_WARNING, "send request failed");
//     }
// }

// void calculate_force(Particle *this_particle1, Particle *this_particle2,
//                      float *force_x, float *force_y, float *force_z)
// {
//     /* Particle_calculate_force */
//     float difference_x, difference_y, difference_z;
//     float distance_squared, distance;
//     float force_magnitude;

//     difference_x = this_particle2->position_x - this_particle1->position_x;
//     difference_y = this_particle2->position_y - this_particle1->position_y;
//     difference_z = this_particle2->position_z - this_particle1->position_z;

//     distance_squared = difference_x * difference_x +
//                        difference_y * difference_y +
//                        difference_z * difference_z;

//     distance = std::sqrt(distance_squared); // sqrtf(distance_squared);

//     force_magnitude = GRAVITATIONAL_CONSTANT * (this_particle1->mass) * (this_particle2->mass) / distance_squared;

//     *force_x = (force_magnitude / distance) * difference_x;
//     *force_y = (force_magnitude / distance) * difference_y;
//     *force_z = (force_magnitude / distance) * difference_z;
// }

// // Parallel
// void nbody(Particle *d_particles, Particle *output)
// {

// #pragma omp parallel
//     {
// #pragma omp for
//         for (int id = 0; id < number_of_particles; id++)
//         {
//             Particle *this_particle = &output[id];

//             float force_x = 0.0f, force_y = 0.0f, force_z = 0.0f;
//             float total_force_x = 0.0f, total_force_y = 0.0f, total_force_z = 0.0f;

//             int i;

//             // #pragma omp parallel reduction(+:total_force_x) reduction(+:total_force_y) reduction(+:total_force_z)
//             {
//                 // #pragma omp for private(force_x) private(force_y) private(force_z)
//                 for (i = 0; i < number_of_particles; i++)
//                 {
//                     if (i != id)
//                     {
//                         calculate_force(d_particles + id, d_particles + i, &force_x, &force_y, &force_z);

//                         total_force_x += force_x;
//                         total_force_y += force_y;
//                         total_force_z += force_z;
//                     }
//                 }
//             }

//             float velocity_change_x, velocity_change_y, velocity_change_z;
//             float position_change_x, position_change_y, position_change_z;

//             this_particle->mass = d_particles[id].mass;

//             velocity_change_x = total_force_x * (time_interval / this_particle->mass);
//             velocity_change_y = total_force_y * (time_interval / this_particle->mass);
//             velocity_change_z = total_force_z * (time_interval / this_particle->mass);

//             position_change_x = d_particles[id].velocity_x + velocity_change_x * (0.5 * time_interval);
//             position_change_y = d_particles[id].velocity_y + velocity_change_y * (0.5 * time_interval);
//             position_change_z = d_particles[id].velocity_z + velocity_change_z * (0.5 * time_interval);

//             this_particle->velocity_x = d_particles[id].velocity_x + velocity_change_x;
//             this_particle->velocity_y = d_particles[id].velocity_y + velocity_change_y;
//             this_particle->velocity_z = d_particles[id].velocity_z + velocity_change_z;

//             this_particle->position_x = d_particles[id].position_x + position_change_x;
//             this_particle->position_y = d_particles[id].position_y + position_change_y;
//             this_particle->position_z = d_particles[id].position_z + position_change_z;
//         }
//     }
// }
// void sub_nbody(Particle *d_particles, Particle *output, int id)
// {
//     Particle *this_particle = &output[id];
//     float force_x = 0.0f, force_y = 0.0f, force_z = 0.0f;
//     float total_force_x = 0.0f, total_force_y = 0.0f, total_force_z = 0.0f;

//     int i;

//     for (i = 0; i < number_of_particles; i++)
//     {
//         if (i != id)
//         {
//             calculate_force(d_particles + id, d_particles + i, &force_x, &force_y, &force_z);

//             total_force_x += force_x;
//             total_force_y += force_y;
//             total_force_z += force_z;
//         }
//     }

//     float velocity_change_x, velocity_change_y, velocity_change_z;
//     float position_change_x, position_change_y, position_change_z;

//     this_particle->mass = d_particles[id].mass;

//     velocity_change_x = total_force_x * (time_interval / this_particle->mass);
//     velocity_change_y = total_force_y * (time_interval / this_particle->mass);
//     velocity_change_z = total_force_z * (time_interval / this_particle->mass);

//     position_change_x = d_particles[id].velocity_x + velocity_change_x * (0.5 * time_interval);
//     position_change_y = d_particles[id].velocity_y + velocity_change_y * (0.5 * time_interval);
//     position_change_z = d_particles[id].velocity_z + velocity_change_z * (0.5 * time_interval);

//     this_particle->velocity_x = d_particles[id].velocity_x + velocity_change_x;
//     this_particle->velocity_y = d_particles[id].velocity_y + velocity_change_y;
//     this_particle->velocity_z = d_particles[id].velocity_z + velocity_change_z;

//     this_particle->position_x = d_particles[id].position_x + position_change_x;
//     this_particle->position_y = d_particles[id].position_y + position_change_y;
//     this_particle->position_z = d_particles[id].position_z + position_change_z;
// }
// void nbody2(Particle *d_particles, Particle *output)
// {
//     // create 4 threads
//     std::thread threads[4];

//     for (int id = 0; id < number_of_particles; id++)
//     {
//         int thread_id = id % 4;
//         threads[thread_id] = std::thread(sub_nbody, d_particles, output, id);
//         threads[thread_id].join();
//     }
// }

// void particle_print(Particle *particle, int number_of_particles)
// {
//     printf("\n\nPrinting %d particles:\n", number_of_particles);
//     for (int i = 0; i < 5; i++)
//     {
//         printf("Particle %d:\n", i);
//         printf("Mass: %f\n", particle[i].mass);
//         printf("Position: (%f, %f, %f)\n", particle[i].position_x, particle[i].position_y, particle[i].position_z);
//         printf("Velocity: (%f, %f, %f)\n", particle[i].velocity_x, particle[i].velocity_y, particle[i].velocity_z);
//     }
// }

// void Solve_MSI()
// {
    

//     Particle *particle_array = nullptr;
//     Particle *particle_array2 = nullptr;
//     Particle *particle_array3 = nullptr;


//     particle_array = Particle_array_construct(number_of_particles);
//     particle_array2 = Particle_array_construct(number_of_particles);
//     particle_array3 = Particle_array_construct(number_of_particles);
//     Particle_array_initialize(particle_array, number_of_particles);

//     printf("Processando simulação NBody....\n");

//     long start = wtime();

//     for (int timestep = 1; timestep <= number_of_timesteps; timestep++)
//     {
//         nbody(particle_array, particle_array2);
//         // nbody2(particle_array, particle_array3);
//         particle_print(particle_array2, number_of_particles);

//         particle_print(particle_array3, number_of_particles);

//         /* swap arrays */
//         Particle *tmp = particle_array;
//         particle_array = particle_array2;
//         particle_array2 = tmp;

//         printf("   Iteração %d OK\n", timestep);
//     }

//     long end = wtime();
//     double time = (end - start) / 1000000.0;

//     printf("Simulação NBody executada com sucesso.\n");
//     printf("Nro. de Partículas: %d\n", number_of_particles);
//     printf("Nro. de Iterações: %d\n", number_of_timesteps);
//     printf("Tempo: %.8f segundos\n", time);

// #ifdef VERBOSE
//     // Imprimir saída para arquivo
//     printf("\nImprimindo saída em arquivo...\n");
//     FILE *fileptr = fopen("nbody_simulation.out", "w");
//     Particle_array_output_xyz(fileptr, particle_array, number_of_particles);
//     printf("Saída da simulação salva no arquivo nbody_simulation.out\n");
// #endif

//     particle_array = Particle_array_destruct(particle_array, number_of_particles);
//     particle_array2 = Particle_array_destruct(particle_array2, number_of_particles);
// }

// /* main */
// int main(int argc, char **argv)
// {
//     if (argc < 2)
//     {
//         std::cout << "Informe um arquivo com os parâmetros de entrada: ./nbody_simulation <input_file.in>\n";
//         std::abort();
//     }
//     FILE *input_data = fopen(argv[1], "r");
//     Particle_input_arguments(input_data);
    
//     srand(time(NULL));
//     curlist = ibv_get_device_list(NULL);
//     Create_master();
//     for (int i = 0; i < parrallel_num + 2; ++i)
//         Create_worker();
//     if (no_run == 1)
//     {
//         Solve_MSI();
//     }
//     else if (no_run == 2)
//     {
//         // Solve_RC();
//     }
//     return PROGRAM_SUCCESS_CODE;
// }

// /*
//  * Get command line arguments.
//  */
// void Particle_input_arguments(FILE *input)
// {
//     number_of_particles = DEFAULT_NUMBER_OF_PARTICLES;
//     block_size = DEFAULT_NUMBER_OF_PARTICLES;
//     domain_size_x = DEFAULT_DOMAIN_SIZE_X;
//     domain_size_y = DEFAULT_DOMAIN_SIZE_Y;
//     domain_size_z = DEFAULT_DOMAIN_SIZE_Z;
//     time_interval = DEFAULT_TIME_INTERVAL;
//     number_of_timesteps = DEFAULT_NUMBER_OF_PARTICLES;
//     timesteps_between_outputs = DEFAULT_TIMESTEPS_BETWEEN_OUTPUTS;
//     execute_serial = DEFAULT_EXECUTE_SERIAL;
//     random_seed = DEFAULT_RANDOM_SEED;
//     mass_maximum = DEFAULT_MASS_MAXIMUM;

//     if (fscanf(input, "%d", &number_of_particles) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read number of particles from standard input!\n");
//         std::abort();
//     }

//     if (number_of_particles < 1)
//     {
//         fprintf(stderr, "ERROR: cannot have %d particles!\n", number_of_particles);
//         std::abort();
//     }

//     if (number_of_particles == 1)
//     {
//         fprintf(stderr, "There is only one particle, therefore no forces.\n");
//         std::abort();
//     }
//     //
//     if (fscanf(input, "%d", &block_size) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read block size from standard input!\n");
//         std::abort();
//     }

//     if (block_size <= 0)
//     {
//         fprintf(stderr, "ERROR: cannot have %d as block size!\n", block_size);
//         std::abort();
//     }

//     if (number_of_particles % block_size != 0)
//     {
//         fprintf(stderr, "ERROR: block size must be divisable by number of particles!\n");
//         std::abort();
//     }

//     if (fscanf(input, "%f", &domain_size_x) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read domain size X from standard input!\n");
//         std::abort();
//     }

//     if (domain_size_x <= 0.0)
//     {
//         fprintf(stderr, "ERROR: cannot have a domain whose X dimension has length %f!\n", domain_size_x);
//         std::abort();
//     }

//     if (fscanf(input, "%f", &domain_size_y) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read domain size Y from standard input!\n");
//         std::abort();
//     }

//     if (domain_size_y <= 0.0)
//     {
//         fprintf(stderr, "ERROR: cannot have a domain whose Y dimension has length %f!\n", domain_size_y);
//         std::abort();
//     }

//     if (fscanf(input, "%f", &domain_size_z) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read domain size Z from standard input!\n");
//         std::abort();
//     }

//     if (domain_size_z <= 0.0)
//     {
//         fprintf(stderr, "ERROR: cannot have a domain whose Z dimension has length %f!\n", domain_size_z);
//         std::abort();
//     }

//     if (fscanf(input, "%f", &time_interval) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read time interval from standard input!\n");
//         std::abort();
//     }

//     if (time_interval <= 0.0)
//     {
//         fprintf(stderr, "ERROR: cannot have a time interval of %f!\n", time_interval);
//         std::abort();
//     }

//     if (fscanf(input, "%d", &number_of_timesteps) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read number of timesteps from standard input!\n");
//         std::abort();
//     }

//     if (number_of_timesteps <= 0)
//     {
//         fprintf(stderr, "ERROR: cannot have %d timesteps!\n", number_of_timesteps);
//         std::abort();
//     }

//     if (fscanf(input, "%d", &timesteps_between_outputs) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read timesteps between outputs from standard input!\n");
//         std::abort();
//     }

//     if (timesteps_between_outputs <= 0)
//     {
//         fprintf(stderr, "ERROR: cannot have %d timesteps between outputs!\n", timesteps_between_outputs);
//         std::abort();
//     }

//     int aux_serial;
//     if (fscanf(input, "%d", &aux_serial) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read serial from standard input!\n");
//         std::abort();
//     }

//     if (aux_serial != 0 && aux_serial != 1)
//     {
//         fprintf(stderr, "ERROR: serial must be 0 (false) or 1 (true)!\n");
//         std::abort();
//     }
// #ifdef VERBOSE
//     execute_serial = (aux_serial == 0) ? false : true;
// #endif

//     if (fscanf(input, "%d", &random_seed) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read random seed from standard input!\n");
//         std::abort();
//     }

//     if (fscanf(input, "%f", &mass_maximum) != 1)
//     {
//         fprintf(stderr, "ERROR: cannot read mass maximum from standard input!\n");
//         std::abort();
//     }

//     if (mass_maximum <= 0.0)
//     {
//         fprintf(stderr, "ERROR: cannot have a maximum mass of %f!\n", mass_maximum);
//         std::abort();
//     }

//     fgetc(input);
//     fgets(base_filename, DEFAULT_STRING_LENGTH, input);
//     if (base_filename[strlen(base_filename) - 1] == '\n')
//     {
//         base_filename[strlen(base_filename) - 1] = '\0';
//     }
// }

// /*
//  * Clear the particle's data.
//  */
// void Particle_clear(Particle *this_particle)
// {
// #ifdef CHECK
//     Particle_check(this_particle, "clear", "Particle_clear");
// #endif

//     this_particle->position_x = 0.0;
//     this_particle->position_y = 0.0;
//     this_particle->position_z = 0.0;
//     this_particle->velocity_x = 0.0;
//     this_particle->velocity_y = 0.0;
//     this_particle->velocity_z = 0.0;
//     this_particle->mass = 0.0;
// }

// /*
//  * Construct the particle.
//  */
// void Particle_construct(Particle *this_particle)
// {
// #ifdef CHECK
//     Particle_check(this_particle, "construct", "Particle_construct");
// #endif

//     Particle_clear(this_particle);
// }

// /*
//  * Destroy the particle.
//  */
// void Particle_destruct(Particle *this_particle)
// {
// #ifdef CHECK
//     Particle_check(this_particle, "destruct", "Particle_destruct");
// #endif

//     Particle_clear(this_particle);
// }

// /*
//  * Initialize the particle by setting its data randomly.
//  */
// void Particle_set_position_randomly(Particle *this_particle)
// {
// #ifdef CHECK
//     Particle_check(this_particle, "randomly set the position", "Particle_set_randomly");
// #endif

//     this_particle->position_x = domain_size_x * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
//     this_particle->position_y = domain_size_y * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
//     this_particle->position_z = domain_size_z * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));
// }

// /*
//  * Initialize the particle by setting its data randomly.
//  */
// void Particle_initialize_randomly(Particle *this_particle)
// {
// #ifdef CHECK
//     Particle_check(this_particle, "randomly initialize", "Particle_initialize_randomly");
// #endif

//     Particle_clear(this_particle);
//     Particle_set_position_randomly(this_particle);

//     this_particle->mass = mass_maximum * (static_cast<float>(random()) / (static_cast<float>(RAND_MAX) + 1.0));

// #ifdef CHECK_VAL
//     printf("mass %g\n", this_particle->mass);
// #endif
// }

// /*
//  * Initialize the particle.
//  */
// void Particle_initialize(Particle *this_particle)
// {
// #ifdef CHECK
//     Particle_check(this_particle, "initialize", "Particle_initialize");
// #endif

//     Particle_initialize_randomly(this_particle);
// }

// void Particle_output(FILE *fileptr, Particle *this_particle)
// {
//     fprintf(fileptr, "%g %g %g %g %g %g %g\n",
//             this_particle->position_x,
//             this_particle->position_y,
//             this_particle->position_z,
//             this_particle->velocity_x,
//             this_particle->velocity_y,
//             this_particle->velocity_z,
//             this_particle->mass);
// }

// void Particle_output_xyz(FILE *fileptr, Particle *this_particle)
// {
//     fprintf(fileptr, "C %g %g %g\n",
//             this_particle->position_x, this_particle->position_y, this_particle->position_z);
// }

// /*
//  * Allocate and return an array of particles.
//  */
// Particle *Particle_array_allocate(int number_of_particles)
// {
//     Particle *this_particle_array = nullptr;

// #ifdef CHECK
//     if (number_of_particles < 0)
//     {
//         fprintf(stderr, "ERROR: illegal number of particles %d to allocate\n", number_of_particles);
//         fprintf(stderr, "  in Particle_array_construct\n");
//         std::abort();
//     }
// #endif

//     if (number_of_particles == 0)
//         return nullptr;

//     this_particle_array = new Particle[number_of_particles];
//     if (this_particle_array == nullptr)
//     {
//         fprintf(stderr, "ERROR: can't allocate a particle array of %d particles\n", number_of_particles);
//         fprintf(stderr, "  in Particle_array_construct\n");
//         std::abort();
//     }

//     return this_particle_array;
// }

// /*
//  * Construct and return an array of particles, cleared.
//  */
// Particle *Particle_array_construct(int number_of_particles)
// {
//     Particle *this_particle_array = nullptr;

//     this_particle_array = Particle_array_allocate(number_of_particles);

//     for (int index = 0; index < number_of_particles; index++)
//     {
//         // Particle_construct(particle_array,index);
//         Particle_construct(&(this_particle_array[index]));
//     }
//     return this_particle_array;
// }

// /*
//  * Deallocate the array of particles, and return NULL.
//  */
// Particle *Particle_array_deallocate(Particle *this_particle_array, int number_of_particles)
// {
// #ifdef CHECK
//     Particle_array_check(this_particle_array, number_of_particles, "deallocate", "Particle_array_deallocate");
// #endif

//     delete[] this_particle_array;
//     this_particle_array = nullptr;

//     return nullptr;
// }

// /*
//  * Destroy the array of particles, and return NULL.
//  */
// Particle *Particle_array_destruct(Particle *this_particle_array, int number_of_particles)
// {
// #ifdef CHECK
//     Particle_array_check(this_particle_array, number_of_particles, "destroy", "Particle_array_destruct");
// #endif

//     for (int index = number_of_particles - 1; index >= 0; index--)
//     {
//         Particle_destruct(&(this_particle_array[index]));
//     }

//     return Particle_array_deallocate(this_particle_array, number_of_particles);
// }

// /*
//  * Initialize the array of particles by setting its data randomly.
//  */
// void Particle_array_initialize_randomly(Particle *this_particle_array, int number_of_particles)
// {
// #ifdef CHECK
//     Particle_array_check(this_particle_array, number_of_particles,
//                          "initialize randomly", "Particle_array_initialize_randomly");
// #endif

//     for (int index = 0; index < number_of_particles; index++)
//     {
//         Particle_initialize_randomly(&(this_particle_array[index]));
//     }
// }

// /*
//  * Initialize the array of particles.
//  */
// void Particle_array_initialize(Particle *this_particle_array, int number_of_particles)
// {
//     Particle_array_initialize_randomly(this_particle_array, number_of_particles);
// }

// /*
//  * Particle_array_output
//  */
// void Particle_array_output(char *base_filename, Particle *this_particle_array, int number_of_particles, int timestep)
// {
//     FILE *fileptr = nullptr;
//     char *filename = nullptr;
//     int filename_length;

// #ifdef CHECK
//     Particle_array_check(this_particle_array, number_of_particles, "output", "Particle_array_output");
// #endif /* #ifdef CHECK */

//     filename_length = strlen(base_filename) + 1 + 8 + 1 + 3;
//     filename = new char[filename_length + 1];

//     if (filename == nullptr)
//     {
//         fprintf(stderr, "ERROR: can't allocate the filename string\n");
//         fprintf(stderr, "  %s_%8.8d.txt\n", base_filename, timestep);
//         fprintf(stderr, "  in Particle_array_output\n");

//         std::abort();
//     } /* if (filename == (char*)NULL) */

//     sprintf(filename, "%s_%8.8d.txt", base_filename, timestep);

//     fileptr = fopen(filename, "w");
//     if (fileptr == nullptr)
//     {
//         fprintf(stderr, "ERROR: can't open the output file named\n");
//         fprintf(stderr, "  %s\n", filename);
//         fprintf(stderr, "  in Particle_array_output\n");

//         std::abort();
//     } /* if (fileptr == (FILE*)NULL) */

//     fprintf(fileptr, "%d %d %d %g %g %g %g %g %d\n",
//             number_of_particles, number_of_timesteps, timesteps_between_outputs,
//             domain_size_x, domain_size_y, domain_size_z,
//             mass_maximum, time_interval,
//             random_seed);

//     fprintf(fileptr, "%d\n", timestep);

//     for (int index = 0; index < number_of_particles; index++)
//     {
//         Particle_output(fileptr, &(this_particle_array[index]));
//     } /* for index */

//     if (fclose(fileptr) != 0)
//     {
//         fprintf(stderr, "ERROR: can't close the output file named\n");
//         fprintf(stderr, "  %s\n", filename);
//         fprintf(stderr, "  in Particle_array_output\n");

//         std::abort();
//     } /* if (fclose(fileptr) != 0) */

//     fileptr = nullptr;
//     delete[] filename;
//     filename = nullptr;
// } /* Particle_array_output */

// /* Outputs particle positions in a format that VMD can easily visualize. */
// void Particle_array_output_xyz(FILE *fileptr, Particle *this_particle_array, int number_of_particles)
// {
//     fprintf(fileptr, "%d\nNBody\n", number_of_particles);

//     for (int index = 0; index < number_of_particles; index++)
//     {
//         Particle_output_xyz(fileptr, &(this_particle_array[index]));
//     }
// }

// #ifdef CHECK
// /*
//  * Check that the particle exists.
//  */
// void Particle_check(Particle *this_particle, char *action, char *routine)
// {
//     if (this_particle != (Particle *)NULL)
//         return;

//     fprintf(stderr, "ERROR: can't %s a nonexistent particle\n",
//             ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
//     fprintf(stderr, "  in %s\n",
//             ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

//     std::abort();
// }

// void Particle_array_check(Particle *this_particle_array, int number_of_particles,
//                           char *action, char *routine)
// {
//     if (number_of_particles < 0)
//     {
//         fprintf(stderr, "ERROR: illegal number of particles %d\n", number_of_particles);
//         fprintf(stderr, "  to %s\n",
//                 ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
//         fprintf(stderr, "  in %s\n",
//                 ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

//         std::abort();
//     }

//     if (number_of_particles == 0)
//     {
//         if (this_particle_array == (Particle *)NULL)
//             return (Particle *)NULL;

//         fprintf(stderr, "ERROR: can't %s\n",
//                 ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
//         fprintf(stderr, "  an existing particle array of length 0\n");
//         fprintf(stderr, "  in %s\n",
//                 ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

//         std::abort();
//     }

//     if (this_particle_array == (Particle *)NULL)
//     {
//         fprintf(stderr, "ERROR: can't %s\n",
//                 ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
//         fprintf(stderr, "  a nonexistent array of %d particles\n", number_of_particles);
//         fprintf(stderr, "  in %s\n",
//                 ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

//         std::abort();
//     }
// }
// #endif /* #ifdef CHECK */

// /* wall_time */
// long wtime()
// {
//     struct timeval t;
//     gettimeofday(&t, NULL);
//     return t.tv_sec * 1000000 + t.tv_usec;
// }
