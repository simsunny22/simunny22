#! /bin/bash
## 修改项目debug 或 error的启动
## ./debug.sh debug| ./debug error

Hive=/opt/vega/vega_hive/start.sh
Scylla=/opt/vega/scylla/start.sh
Etcd=""
Moniker=/opt/vega/vega_moniker/bin/
Swf=/opt/vega/vega_swf/bin/
Cuckoo1=/etc/vega_confs/vega_cuckoo.conf
Cuckoo2=/opt/vega/gopath/src/vega_cuckoo/cmd/cuckood/vega_cuckoo.conf
Ganesha=""
Pithos=/opt/vega/vega_pithos/bin/
Watch=/opt/vega/vega_watch/bin/
Sniffer=""
Fabricd=/opt/vega/vega_fabricd/bin/


fun1(){
  Path=$1
  Level=$2

  if [ "$Level"x = "debug"x ];then
    sed -i 's/--default-log-level=error/--default-log-level=debug/g' $Path
  fi
  
  
  if [ "$Level"x = "error"x ];then
    sed -i 's/--default-log-level=debug/--default-log-level=error/g' $Path
  fi

}

fun2(){
  Path=$1
  Level=$2
  Files=($(find $Path -name "*"|grep "config"))
  for file in ${Files[@]}
  do
    if [ "$Level"x = "debug"x ];then
      sed -i 's/\[error/\[debug/g' $file
    fi

    if [ "$Level"x = "error"x ];then
      sed -i 's/\[debug/\[error/g' $file
    fi

  done
}




#hive
echo $Hive......
fun1 $Hive $1


#scylla
echo $Scylla ......
fun1 $Scylla $1


#etcd ......

#monier
echo $Moniker .....
fun2 $Moniker $1


#swf
echo $Swf ......
fun2 $Swf $1


#Cuckoo
echo $Cuckoo1 ......

if [ "$Level"x = "debug"x ];then
  sed -i 's/debug: false/debug: true/g' $Cuckoo1
  sed -i 's/debug: false/debug: true/g' $Cuckoo2
fi


if [ "$Level"x = "error"x ];then
  sed -i 's/debug: true/debug: false/g' $Cuckoo1
  sed -i 's/debug: true/debug: false/g' $Cuckoo2
fi


#ganesha ......


#pithos
echo $Pithos ......
fun2 $Pithos $1

#watch
echo $Watch ......
fun2 $Watch $1

#sniffer ......

#Fabricd
echo $Fabricd ......
fun2 $Fabricd $1
