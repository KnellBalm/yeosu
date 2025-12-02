#!/bin/bash

WORKDIR=module/

DAEMON=pop.py

LOG=${WORKDIR}/log/startup.log


if ! [ -d ${WORKDIR}/log ]; then
    mkdir ${WORKDIR}/log
fi

function do_start()
{
	cd ${WORKDIR}
    	nohup python -u ${DAEMON} >> ${LOG} &
}

function do_stop()
{
        PID=`ps -ef | grep ${DAEMON} | grep -v grep | awk '{print $2}'`
        if [ "$PID" != "" ]; then
                kill -9 $PID
                echo '>>>>' * ${DAEMON} $PID 'killed'
        fi
}

case "$1" in
    start|stop)
        do_${1}
        echo '>>>> PYTHON MODULE STARTED'
        ;;
    reload|restart)
        do_stop
        do_start
        ;;
    *)
        exit 1
        ;;
esac

