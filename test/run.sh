rm -rf t t1 t2 t3
iteration=1
echo "" > output.txt
make t

for i in {1..10}
do
    # print i
    echo $i
    # ./t2 $iteration >> output.txt 
    ./t $iteration >> output.txt
done





