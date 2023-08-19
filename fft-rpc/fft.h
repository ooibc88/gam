
#pragma once
#include <complex>

#define PI acos(-1)
typedef std::complex<float> Complex;



void Get_curlist();
void Create_Config(int is_master, const char* ip_master, const char* ip_worker, int port_master, int port_worker);
void Create_Alloc();
void Create_master();
void Create_worker(int is_master);
void sub_fft(uint32_t work_id, uint64_t addr_value, unsigned int n, unsigned int l, unsigned int k, Complex T);
void add(uint32_t work_id, uint64_t addr_value, uint32_t a);
void Solve_MSI();
void Solve_add();
void Solve_add_alloc();
