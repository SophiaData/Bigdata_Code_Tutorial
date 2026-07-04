#!/bin/bash
# Grant replication privileges to cdc_user (only on source database)
mysql -u root -proot -e "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, SHOW DATABASES, LOCK TABLES ON *.* TO 'cdc_user'@'%'; FLUSH PRIVILEGES;" 2>/dev/null || true
