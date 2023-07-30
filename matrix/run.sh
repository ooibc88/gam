rm -rf t t1 t2 t3
iteration=1
echo "" > output.txt
make t3

for i in {1..3}
do
    # print i
    echo $i
    # ./t2 $iteration >> output.txt 
    ./t3 $iteration >> output.txt
done