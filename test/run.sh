rm -rf t
iteration=1
echo "" > output.txt
make t
./t $iteration >> output.txt 
# ./t $iteration