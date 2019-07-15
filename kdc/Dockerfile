FROM centos:6
RUN yum install krb5-server krb5-libs krb5-workstation -y
COPY . /kdc
# CMD ls /etc/rc.d/init.d
CMD sh kdc/install_kdc.sh
EXPOSE 88
