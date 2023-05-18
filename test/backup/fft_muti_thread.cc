#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>
#include <thread>
#include <pthread.h>
#include <complex>

using namespace std;
#define PI acos(-1)
#define N 64       // FFT点数
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
float xn[N];       // 采样信号序列

void dit_r2_fft(float *xn, int n, int stride, complex<float> *Xk)
{
    complex<float> X1k[n / 2];
    complex<float> X2k[n / 2];

    if (n == 1)
    {
        // 递归终止
        Xk[0].real(xn[0]);
        Xk[0].imag(0);
    }
    else
    {
        if (n == 64)
        {
            thread t1(dit_r2_fft, xn, n / 2, 2 * stride, (complex<float> *)X1k);
            thread t2(dit_r2_fft, xn + stride, n / 2, 2 * stride, (complex<float> *)X2k);
            t1.join();
            t2.join();
        }
        else
        {
            // 偶数n/2点DFT
            dit_r2_fft(xn, n / 2, 2 * stride, X1k);
            // 奇数n/2点DFT
            dit_r2_fft((float *)(xn + stride), n / 2, 2 * stride, X2k);
        }

        // 蝶形运算
        for (int k = 0; k <= n / 2 - 1; k++)
        {
            complex<float> t;
            complex<float> WNk;

            // 这种求WNk的方法会消耗大量时间，必须优化
            WNk.real(cos(2 * PI / n * k));
            WNk.imag(-sin(2 * PI / n * k));

            // complex_mul(&t, &WNk, &X2k[k]);
            // complex_add(&Xk[k], &X1k[k], &t);
            // complex_sub(&Xk[k + n / 2], &X1k[k], &t);
            // stl 的complex库中已经重载了*运算符，可以直接用
            t = WNk * X2k[k];
            Xk[k] = X1k[k] + t;
            Xk[k + n / 2] = X1k[k] - t;
        }
    }
}

int main()
{
    complex<float> Xk[N];
    complex<float> Xk1[N / 2];
    complex<float> Xk2[N / 2];
    float Xabs[N]; // 频率序列绝对值（模）

    // 制作采样序列，实际当中由采样电路和采样程序获得
    for (int i = 0; i < N; i++)
    {
        // S = 0.7*sin(2*pi*50*t) + sin(2*pi*120*t);
        xn[i] = 0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i);
        // X = S + 2*randn(size(t));
        // xn[i] = xn[i] + 2 * (rand() % 1000) / 1000.0 - 1;
    }
    // print xn

    for (int i = 0; i < N; i++)
    {
        printf("xn[%d] = %f ", i, xn[i]);
    }
    printf("\n");

    // FFT
    dit_r2_fft(xn, N, 1, Xk);


    for (int i = 0; i < N; i++)
    {
        printf("Xk[%d] = %f + j%f\n", i, Xk[i].real(), Xk[i].imag());
    }
    return 0;
}