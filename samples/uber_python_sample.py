from vertica_python_project import vertica_python
from connection_info import CONN_INFO

#sample to connect and run vertica queries
with vertica_python.connect(**CONN_INFO) as conn:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM hello_world')
    for row in cursor.iterate():
        print(row)
