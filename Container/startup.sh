#!/bin/bash

EPICONDUCTOR_COLLECTION_DATE=$(cat /EPICONDUCTOR_COLLECTION_DATE.txt)
echo "EPICONDUCTOR_COLLECTION_DATE=$EPICONDUCTOR_COLLECTION_DATE" >> $R_HOME/etc/Renviron.site
echo "export EPICONDUCTOR_COLLECTION_DATE=$EPICONDUCTOR_COLLECTION_DATE" >> /etc/profile.d/EPICONDUCTOR_COLLECTION_DATE.sh

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

# # start the MariaDB services
start-services

wait