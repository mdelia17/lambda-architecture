#!/bin/bash

echo 'executing hive job '$1
docker exec -i -t -u root hive-server /bin/bash /opt/hive/bin/hive --f /hive/$1.hql