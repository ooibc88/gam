cd ../src
make clean
make -j
cd ../fft
rm -rf t t1 t2 t3
iteration=1
no_run=1
echo "" > output.txt
make t3

for i in {1..1}
do
    # print i
    echo $i
    # ./t2 $iteration >> output.txt 
    ./t3 $iteration $no_run >> output.txt
done