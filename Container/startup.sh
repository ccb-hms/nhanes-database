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

# start sshd
/usr/sbin/sshd -D&

# start the MariaDB services in the background
start-services&

# provision the MariaDB Columnstore service when it becomes available
provision

wait