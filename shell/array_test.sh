#! /bin/bash

# 1 the bracket represent array, split by blank
array=(v1 v2 v3 v4)

# 2 get array value 'v=${array[1]}'
v=${array[1]}

# 3 get array all value 'v=${array[*]} v=${array[@]}'
echo "all1: ${array[*]}"
echo "all2: ${array[@]}"

# 4 get arry lenght
echo "length1: ${#array[*]}"
echo "length2: ${#array[@]}"
