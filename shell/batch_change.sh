#! /bin/bash

# 1. filter regular file by keyword
files=$(grep 'hive/primary_journal.hh' -rn *| grep -v 'batch_change.sh')

# 2. replace keyword by you wanted
for file in ${files[@]}
do 
  name=${file%%:*}
  if [[ $name == *'hive/primary_journal.hh'* ]]
  then
    echo ""
  else
     echo $name
     sed -i  's/hive\/primary_journal.hh/hive\/journal\/primary_journal.hh/g' $name
  fi
done
