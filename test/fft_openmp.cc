#include <bits/stdc++.h>
#include <complex>
#include <valarray>
#include <omp.h>

using namespace std;

// #define THREADS 4
const float PI = acos(-1);

#define Fs 1000
#define L 4096
#define dt (1.0 / Fs)

typedef std::complex<float> Complex;
typedef std::valarray<Complex> CArray;

void fft(CArray &x)
{
    // DFT
    unsigned int N = x.size(), k = N, n;
    float thetaT = PI / N;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT)), T;
    while (k > 1)
    {
        n = k;
        k >>= 1;
        phiT = phiT * phiT;
        T = 1.0L;
        // #pragma omp parallel for num_threads(THREADS) schedule(static)
        {
            for (unsigned int l = 0; l < k; l++)
            {
                for (unsigned int a = l; a < N; a += n)
                {
                    unsigned int b = a + k;
                    Complex t = x[a] - x[b];
                    x[a] += x[b];
                    x[b] = t * T;
                }
                T *= phiT;
            }
        }
    }
    // Decimate
    unsigned int m = (unsigned int)log2(N);

    // #pragma omp parallel for num_threads(THREADS)
    for (unsigned int a = 0; a < N; a++)
    {
        unsigned int b = a;
        // Reverse bits
        b = (((b & 0xaaaaaaaa) >> 1) | ((b & 0x55555555) << 1));
        b = (((b & 0xcccccccc) >> 2) | ((b & 0x33333333) << 2));
        b = (((b & 0xf0f0f0f0) >> 4) | ((b & 0x0f0f0f0f) << 4));
        b = (((b & 0xff00ff00) >> 8) | ((b & 0x00ff00ff) << 8));
        b = ((b >> 16) | (b << 16)) >> (32 - m);
        if (b > a)
        {
            Complex t = x[a];
            x[a] = x[b];
            x[b] = t;
        }
    }
}

void ifft(CArray &x)
{
    // conjugate the complex numbers
    x = x.apply(std::conj);

    // forward fft
    fft(x);

    // conjugate the complex numbers again
    x = x.apply(std::conj);

    // scale the numbers
    x /= x.size();
}

int main()
{
    // cin>>n;
    // const Complex test[] = {{1.0,1.0},{0.0,1.0},{2.0,1.0},{1.1,4.1},{3.1,5.0},{6.0,6.0},{12.0,3.0},{1.0,1.0}};
    // const Complex test[] = { 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0 };
    Complex test[L];
    // S = 0.7*sin(2*pi*50*t) + sin(2*pi*120*t);
    for (int i = 0; i < L; i++)
    {
        test[i].real(0.7 * sin(2 * PI * 50 * i * dt) + sin(2 * PI * 120 * i * dt));
        test[i].imag(0);
    }
    CArray data(test, L);

    // CArray data = read_data(n);

    // forward fft
    fft(data);

    std::cout << "fft" << std::endl;
    for (int i = 0; i < L; ++i)
    {
        std::cout << data[i] << std::endl;
    }

    return 0;
}