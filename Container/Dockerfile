# Docker container for the NHANES data pull
# docker build --tag nhanesdownload .

FROM hmsccb/analytic-workbench:version-1.1.1

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
	cifs-utils

RUN mkdir /mnt/DataLake
RUN chmod 777 /mnt/DataLake/

#RUN R -e "install.packages('randomForest')"
