#pragma once
#include <complex>
void run_subfft(
    uint32_t work_id, 
    uint64_t addr_value, 
    unsigned int n, 
    unsigned int l, 
    unsigned int k, 
    std::complex<float> T,
    const char* ip_worker);

void run_add(
    uint32_t work_id,
    uint32_t a,
    uint64_t addr_value,
    const char* ip_worker);