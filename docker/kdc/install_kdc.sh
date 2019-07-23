#!/bin/sh
sleep 5

REALM=${1-'EXAMPLE.COM'}
SERVICE_NAME=${2-'vertica'}
USERS=${3-'user1,user2,user3'}
KADMIN='kadmin.local'
# Write conf file
echo "[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log
[libdefaults]
 default_realm = $REALM
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
[realms]
 $REALM = {
  kdc = localhost
  admin_server = localhost
 }
 [domain_realm]
 .company.com = $REALM
 company.com = $REALM" | tee /etc/krb5.conf
kdb5_util -P 'admin' create

systemctl start kadmin.service
systemctl start krb5kdc.service
chkconfig krb5kdc on
chkconfig kadmin on

# Create admin
$KADMIN -q "addprinc -pw admin admin/admin"
echo "*/admin@$REALM *" | tee -a /var/kerberos/krb5kdc/kadm5.acl

# Add user principals
for u in ${USERS//,/ };do
	$KADMIN -q "addprinc -pw ${u} ${u}"
done
