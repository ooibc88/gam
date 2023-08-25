rm -rf main
iteration=1
no_run=1
echo "" > output.txt
make main

for i in {1..1}
do
    # print i
    echo $i
    # ./t2 $iteration >> output.txt 
    # ./main input_files/nbody_input-16384_16384.in >> output.txt
    ./main input_files/nbody_input-100_100.in >> output.txt
done