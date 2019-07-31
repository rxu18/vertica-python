#!/bin/sh
# TODO: Give this file a better name.
install_kdc(){
	echo "Setting up KDC"
	docker network create test
	docker build -t vertica/kdc docker/kdc
	docker run -d --privileged --name=$VP_KDC_NAME --network=test vertica/kdc
	docker exec $VP_KDC_NAME /kdc/install_kdc.sh
}

create_db(){
	echo "Creating database"
	if [ ! -f "docker-vertica/packages/vertica-ce.latest.rpm" ]; then
		export VERTICA_CE_URL="https://s3.amazonaws.com/vertica-community-edition-for-testing/XCz9cp7m/vertica-9.1.1-0.x86_64.RHEL6.rpm"
		git clone https://github.com/jbfavre/docker-vertica.git
		curl $VERTICA_CE_URL --create-dirs -o docker-vertica/packages/vertica-ce.latest.rpm
	fi
	docker build -f docker-vertica/Dockerfile.centos.7_9.x --build-arg VERTICA_PACKAGE=vertica-ce.latest.rpm -t jbfavre/vertica docker-vertica
	docker run -d -p 5433:5433 --name=$VP_DB_NAME --network=test jbfavre/vertica
	
	echo "Making service keytab"
	export V_PRINC=vertica/vertica.example.com@EXAMPLE.COM
	docker exec $VP_KDC_NAME kadmin.local -q "addprinc -randkey ${V_PRINC}"
	docker exec $VP_KDC_NAME kadmin.local -q "ktadd -norandkey -k vertica.keytab ${V_PRINC}"
	docker cp $VP_KDC_NAME:vertica.keytab .
	docker cp vertica.keytab $VP_DB_NAME:/
	rm vertica.keytab

	echo "Waiting for db to start"
	sleep 60

	echo "Kerberize db"
	docker cp docker/kdc/kerberize.sh $VP_DB_NAME:/
	docker exec $VP_DB_NAME yum install -y krb5-workstation
	docker exec $VP_DB_NAME /bin/sh -c "echo $(docker inspect -f '{{.NetworkSettings.Networks.test.IPAddress }}' $VP_KDC_NAME) kerberos.example.com >> /etc/hosts"
	docker exec $VP_DB_NAME /kerberize.sh
}

test_python(){
	# TODO: test_python sends whole packet to daemon. Mounting it would be faster and saves space.
	docker build -f docker/test/Dockerfile -t vertica/test .
	docker run --network=test --rm --name=vp.test vertica/test
	docker image rm vertica/test
}

stop_container(){	
	echo "Stopping containers"
	docker container stop $VP_KDC_NAME
	docker container stop $VP_DB_NAME
}

clean_system(){
	echo "Cleaning up"
	docker container rm $VP_DB_NAME
	docker container rm $VP_DB_NAME
	docker image rm jbfavre/vertica
	docker image rm vertica/kdc
	docker network remove test
}

echo_use(){
	echo "Usage: $0 [arguments] command"
}

echo_help(){
	echo "$0 sets up a kerberos-enabled vertica database to facilitate testing."
	echo "Usage: $0 [arguments] command"
	echo
	echo "Commands: [start|test|stop|clean]"
	echo "Start builds vertica and kerberos."
	echo "Test builds and runs the vertica-python test suite."
	echo "Stop stops the containers."
	echo "Clean removes the containers and the images."
	echo
	echo "Options: --kdc, --db, --py"
	echo "--kdc specifies the name of the kdc container (default: vp.kdc)"
	echo "--db specifies the name of the database container (default: vp.db)"
	echo "--py specifies the version of python. Options: py37 (default),py27,py34,py35,py36."
}

if [ $# -eq 0 ]; then
	echo_use
	return 0
fi

VP_KDC_NAME='vp.kdc'
VP_DB_NAME='vp.db'
while [ -n "$1" ]; do
	case "$1" in
	--help) echo_help
	exit 0
	;;
	--kdc) VP_KDC_NAME=$2
	shift
	;;
	--db) VP_DB_NAME=$2
	shift
	;;
	--py) PYENV=$2
	;;	
	--) shift
	break ;;
	*) break ;;
	esac	
	shift
done

op=$1
if [ op = 'start' ]; then
	install_kdc
	create_db
elif [ $1 = 'test' ]; then
	test_python
elif [ $1 = 'stop' ]; then
	stop_container
elif [ $1 = 'clean' ]; then
	clean_system
else
	echo_use
fi
