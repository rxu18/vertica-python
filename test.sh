#! /bin/sh

echo "Creating KDC"
docker network create test
docker build -t vertica/kdc docker/kdc
docker run -d --privileged --name vp.kdc --network test vertica/kdc
docker exec vp.kdc /kdc/install_kdc.sh

echo "Creating database"
if [! -f "docker-vertica/packages/vertica-ce.latest.rpm"]; then
	export VERTICA_CE_URL="https://s3.amazonaws.com/vertica-community-edition-for-testing/XCz9cp7m/vertica-9.1.1-0.x86_64.RHEL6.rpm"
	git clone https://github.com/jbfavre/docker-vertica.git
	curl $VERTICA_CE_URL --create-dirs -o docker-vertica/packages/vertica-ce.latest.rpm
fi
docker build -f docker-vertica/Dockerfile.centos.7_9.x --build-arg VERTICA_PACKAGE=vertica-ce.latest.rpm -t jbfavre/vertica docker-vertica
docker run -d --name=vp.db --network=test jbfavre/vertica

echo "Making keytabs"
export V_PRINC=vertica/$(docker inspect -f '{{.NetworkSettings.Networks.test.IPAddress }}' vp.db)@EXAMPLE.COM
docker exec vp.kdc kadmin.local -q "ktadd -norandkey -k vertica.keytab ${V_PRINC}"
docker cp vp.kdc:vertica.keytab .

echo "Kerberizing db"
sleep 60
docker cp vertica.keytab vp.db:/
docker cp docker/kdc/kerberize.sh vp.db:/
docker exec vp.db yum install -y krb5-workstation
export KDC_ID=$(docker inspect -f '{{.Id }}' vp.kdc | cut -c -12)
docker exec vp.db /bin/sh "echo $(docker inspect -f '{{.NetworkSettings.Networks.test.IPAddress }}' vp.kdc) $KDC_ID >> /etc/hosts"
docker exec vp.db /kerberize.sh $KDC_ID
docker build -f docker/test/Dockerfile -t vertica/test .
docker run --network=test --name=vp.test vertica/test

echo "Cleaning up"
docker container stop vp.kdc
docker container stop vp.db
docker container rm vp.kdc
docker container rm vp.db
docker container rm vp.test
docker network remove test

