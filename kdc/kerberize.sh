KSN=${1-'vertica'}
KHOST=${2-'localhost'} # Your computer name.
KTAB=${3-`pwd`/vertica.keytab}
REALM=${4-'EXAMPLE.COM'}
dbport=${5-'5433'}

# specify needed params
vsql -a -p $dbport << eof
ALTER DATABASE SET KerberosServiceName = '${KSN}';
ALTER DATABASE SET KerberosRealm = '${REALM}';
ALTER DATABASE SET KerberosKeytabFile = '${KTAB}';
ALTER DATABASE SET KerberosHostName = '{KHOST}';
SELECT kerberos_config_check();

CREATE AUTHENTICATION kerberos  METHOD 'gss' HOST '0.0.0.0/0';
GRANT AUTHENTICATION  kerberos TO public;
eof