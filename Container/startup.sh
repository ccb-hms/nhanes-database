#!/bin/bash

# if username and password were not provided, exit.
# otherwise, create the user, add to groups, and modify file system permissions
if [[ -z $CONTAINER_USER_USERNAME ]] || [[ -z $CONTAINER_USER_PASSWORD ]];
then
      exit 1
else
    useradd $CONTAINER_USER_USERNAME \
	&& echo "$CONTAINER_USER_USERNAME:$CONTAINER_USER_PASSWORD" | chpasswd \
	&& usermod -aG wheel $CONTAINER_USER_USERNAME \
	&& chsh -s /bin/bash ${CONTAINER_USER_USERNAME}
fi

# TODO: remove this when nhanesA gets fixed
# Start SQL Server: this is a hack to make nhanesA cooperate
runuser -m -p  mssql -c '/opt/mssql/bin/sqlservr  --accept-eula --reset-sa-password &'

# start sshd
/usr/sbin/sshd -D&

# # start the MariaDB services
start-services

wait