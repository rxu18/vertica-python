import vertica_python
from connection_info import CONN_INFO

if __name__ == '__main__':
    # simple connection, with manual close
    connection = vertica_python.connect(**CONN_INFO)

    # do things
    cur = connection.cursor()

    # cur.execute('insert into hello_world values (2, \'Hi\')')
    # cur.execute('commit')

    cur.execute('SELECT * FROM hello_world')
    for row in cur.iterate():
        print(row)

    connection.close()
