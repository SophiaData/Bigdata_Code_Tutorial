## Q1: MySQL Error “Too many connections” and how to resolve it

[解决方案原文](https://www.thegeekdiary.com/mysql-error-too-many-connections-and-how-to-resolve-it/)

> Solution

```
$ mysql –u root –p
mysql> SHOW VARIABLES LIKE 'max_connections';
+-----------------+-------+
| Variable_name   | Value |
+-----------------+-------+
| max_connections | 151   |
+-----------------+-------+
```

> Changing the max_connections parameter (Temporarily)

```
$ mysql –u root –p
mysql> SET GLOBAL max_connections = 512;
```

> Changing the max_connections parameter (Permanently)

```
# vi /etc/my.cnf
max_connections = 512
```

- For CentOS/RHEL 6:

```
# service mysqld restart
```

- For CentOS/RHEL 7:

```
# systemctl restart mysqld
```
