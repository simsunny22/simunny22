#!/bin/bash
#------- common start ------------
function log() {
    d=$(date '+%Y-%m-%d %T')
    f=${FUNCNAME[2]}
    l=${BASH_LINENO[1]}
    local level=$1
    local msg=$2
    RED='\e[1;31m'
    GREEN='\e[0;32m' # 绿色
    NC='\e[0m' # 没有颜色
    if [ "${level}" == "INFO" ]; then
        printf "${GREEN}%s [%-5s] $$ %s:%s: %s${NC}\n" "$d" "${level}" "$f" "$l" "${msg}"
    elif [ "${level}" == "ERROR" ]; then
        printf "${RED}%s [%-5s] $$ %s:%s: %s${NC}\n" "$d" "${level}" "$f" "$l" "${msg}"
    fi
}
function log_info() {
    log INFO "$1"
}
function log_error() {
    log ERROR "$1"
    if [ ! -z "$2" ]; then
        if [ $2 -ne 0 ]; then
            exit $2
        fi
    fi
}
#------- common end --------------
function coroutine_by_file()
{
        local cor_num=$1 #并发数
        local func=$2
        local file=$3
        local params=($@)
        local func_params=${params[@]:3}

        # check input params
        if [ ! -f ${file} ]; then
                log_error "${file} not exist"
                return 1
        fi
        expr ${cor_num} + 1 >/dev/null 2>&1
        if [ $? -ne 0 ]; then
                log_error "cor_num is not number"
                return 1
        fi


        [ -e /tmp/fd1 ] || mkfifo /tmp/fd1 #创建有名管道
        exec 3<>/tmp/fd1                   #创建文件描述符，以可读（<）可写（>）的方式关联管道文件，这时候文件描述符3就有了有名管道文件的所有特性
        rm -rf /tmp/fd1                    #关联后的文件描述符拥有管道文件的所有特性,所以这时候管道文件可以删除，我们留下文件描述符来用就可以了

        for ((i=1;i<=${cor_num};i++))
        do
                echo >&3                   #&3代表引用文件描述符3，这条命令代表往管道里面放入了一个"令牌"
        done

        while read line; do
        read -u3                           #代表从管道中读取一个令牌
        {
                eval "${func} \"${line}\" ${func_params[@]}"
                echo >&3                   #代表我这一次命令执行到最后，把令牌放回管道
        } &
        done < ${file}

        wait
        exec 3<&-                       #关闭文件描述符的读
        exec 3>&-                       #关闭文件描述符的写
}
