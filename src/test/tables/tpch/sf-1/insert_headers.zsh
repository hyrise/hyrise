#!/bin/zsh

for file in *.tbl; do 
    echo $file;
    header_file="../sf-0.001/"$file
    echo $header_file;
    head -n 2 $header_file | cat - $file | sponge $file
done
