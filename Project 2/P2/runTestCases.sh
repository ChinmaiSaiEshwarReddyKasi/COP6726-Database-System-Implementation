#!/bin/bash
rm output.txt
./test.out < tc1.txt  >> output.txt
echo "****************************************************************************************************************************************************************************************" >> output.txt
./test.out < tc2.txt  >> output.txt
echo "****************************************************************************************************************************************************************************************" >> output.txt
./test.out < tc3.txt  >> output.txt
