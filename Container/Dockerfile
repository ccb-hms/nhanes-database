#
# Container image for CCB's NHANES database project
# Author: Nathan Palmer
# Copyright: Harvard Medical School
#
#
# docker build --progress plain --tag nhanes-workbench -f Container/Dockerfile .
# docker \
#     run \
#         --rm \
#         --name nhanes-workbench \
#         -d \
#         -v LOCAL_DIRECTORY:/HostData \
#         -p 8787:8787 \
#         -p 2200:22 \
#         -p 1433:1433 \
#         -e 'CONTAINER_USER_USERNAME=test' \
#         -e 'CONTAINER_USER_PASSWORD=test' \
#         -e 'ACCEPT_EULA=Y' \
#         -e 'SA_PASSWORD=yourStrong(!)Password' \
#         nhanes-workbench

FROM mcr.microsoft.com/mssql/server:2019-CU12-ubuntu-20.04

#------------------------------------------------------------------------------
# Basic initial system configuration
#------------------------------------------------------------------------------

USER root

# install standard Ubuntu Server packages
RUN yes | unminimize

# we're going to create a non-root user at runtime and give the user sudo
RUN apt-get update && \
	apt-get -y install sudo \
	&& echo "Set disable_coredump false" >> /etc/sudo.conf
	
# set locale info
RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
	&& apt-get update && apt-get install -y locales \
	&& locale-gen en_US.utf8 \
	&& /usr/sbin/update-locale LANG=en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV TZ=America/New_York

WORKDIR /tmp

#------------------------------------------------------------------------------
# Install system tools and libraries via apt
#------------------------------------------------------------------------------

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
	&& apt-get install \
		-y \
		ca-certificates \
		curl \
		less \
		libgomp1 \
		libpango-1.0-0 \
		libxt6 \
		libsm6 \
		make \
		texinfo \
		libtiff-dev \
		libpng-dev \
		libicu-dev \
		libpcre3 \
		libpcre3-dev \
		libbz2-dev \
		liblzma-dev \
		gcc \
		g++ \
		openjdk-8-jre \
		openjdk-8-jdk \
		gfortran \
		libreadline-dev \
		libx11-dev \
		libcurl4-openssl-dev \ 
		libssl-dev \
		libxml2-dev \
		wget \
		libtinfo5 \
		openssh-server \
		ssh \
		xterm \
		xauth \
		screen \
		tmux \
		git \
		libgit2-dev \
		nano \
		emacs \
		vim \
		man-db \
		zsh \
		unixodbc \
		unixodbc-dev \
		gnupg \
		krb5-user \
		python3-dev \
		python3 \ 
		python3-pip \
		alien \
		libaio1 \
		pkg-config \ 
		libkrb5-dev \
		unzip \
		cifs-utils \
		lsof \
		libnlopt-dev \
		libopenblas-openmp-dev \
		libpcre2-dev \
		systemd \
		libcairo2-dev \
		libharfbuzz-dev \
		libfribidi-dev \
		cmake \
	&& rm -rf /var/lib/apt/lists/*


#------------------------------------------------------------------------------
# Configure system tools
#------------------------------------------------------------------------------

# required for ssh and sshd	
RUN mkdir /var/run/sshd	

# enable password authedtication over SSH
RUN sed -i 's!^#PasswordAuthentication yes!PasswordAuthentication yes!' /etc/ssh/sshd_config

# configure X11
RUN sed -i "s/^.*X11Forwarding.*$/X11Forwarding yes/" /etc/ssh/sshd_config \
    && sed -i "s/^.*X11UseLocalhost.*$/X11UseLocalhost no/" /etc/ssh/sshd_config \
    && grep "^X11UseLocalhost" /etc/ssh/sshd_config || echo "X11UseLocalhost no" >> /etc/ssh/sshd_config	

# tell git to use the cache credential helper and set a 1 day-expiration
RUN git config --system credential.helper 'cache --timeout 86400'


#------------------------------------------------------------------------------
# Install and configure database connectivity components
#------------------------------------------------------------------------------

# install MS SQL Server ODBC driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
	&& echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main" | tee /etc/apt/sources.list.d/mssql-release.list \
	&& apt-get update \
	&& ACCEPT_EULA=Y apt-get install msodbcsql17
	
# install FreeTDS driver
WORKDIR /tmp
RUN wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-1.1.40.tar.gz
RUN tar zxvf freetds-1.1.40.tar.gz
RUN cd freetds-1.1.40 && ./configure --enable-krb5 && make && make install
RUN rm -r /tmp/freetds*

# tell unixodbc where to find the FreeTDS driver shared object
RUN echo '\n\
[FreeTDS]\n\
Driver = /usr/local/lib/libtdsodbc.so \n\
' >> /etc/odbcinst.ini

# install pyodbc
RUN pip3 install pyodbc

#------------------------------------------------------------------------------
# Install and configure R
#------------------------------------------------------------------------------

# declare R version to be installed, make it available at build and run time
ENV R_VERSION_MAJOR 4
ENV R_VERSION_MINOR 2
ENV R_VERSION_BUGFIX 1
ENV R_VERSION $R_VERSION_MAJOR.$R_VERSION_MINOR.$R_VERSION_BUGFIX
ENV R_HOME=/usr/local/lib/R

WORKDIR /tmp
RUN wget https://cran.r-project.org/src/base/R-4/R-$R_VERSION.tar.gz
RUN tar zxvf R-$R_VERSION.tar.gz
# figure out how many cores we should use for compile, and call make -j to do multithreaded build
RUN ["/bin/bash", "-c", "x=$(cat /proc/cpuinfo | grep processor | wc -l) && let ncores=$x-1 && if (( ncores < 1 )); then let ncores=1; fi && echo \"export N_BUILD_CORES=\"$ncores >> /tmp/ncores.txt"]
RUN ["/bin/bash", "-c", "source /tmp/ncores.txt && cd R-$R_VERSION && ./configure -with-blas -with-lapack --enable-R-shlib && make -j $N_BUILD_CORES && make install"]

# Clean up downloaded files
WORKDIR /tmp
RUN rm -r /tmp/R-$R_VERSION*

# set CRAN repository snapshot for standard package installs
ENV R_REPOSITORY=https://cran.microsoft.com/snapshot/2022-10-18
RUN echo 'options(repos = c(CRAN = "'$R_REPOSITORY'"))' >> $R_HOME/etc/Rprofile.site

# enable multithreaded build for R packages
RUN echo 'options(Ncpus = max(c(parallel::detectCores()-1, 1)))' >> $R_HOME/etc/Rprofile.site

# tell R to use cairo
RUN echo 'options(bitmapType="cairo")' >> $R_HOME/etc/Rprofile.site


# tell R to use wget (devtools::install_github aimed at HTTPS connections had problems with libcurl)
RUN echo 'options("download.file.method" = "wget")' >> $R_HOME/etc/Rprofile.site
RUN Rscript -e "install.packages(c('curl', 'httr'))"


#------------------------------------------------------------------------------
# Install basic R packages
#------------------------------------------------------------------------------

# use the remotes package to manage installations
RUN Rscript -e "install.packages('remotes')"

# configure and install rJava
RUN R CMD javareconf
RUN Rscript -e "remotes::install_cran('rJava', type='source')"

# install devtools
RUN Rscript -e "remotes::install_cran('devtools')"

#----------------------------------------------------------------------------------------------------
# install standard data science and bioinformatics packages
RUN Rscript -e "remotes::install_cran('Rcpp')"
RUN Rscript -e "remotes::install_cran('roxygen2')"
RUN Rscript -e "remotes::install_cran('tidyverse')"
RUN Rscript -e "remotes::install_cran('git2r')"
RUN Rscript -e "remotes::install_cran('getPass')"
RUN Rscript -e "remotes::install_cran('xlsx')"
RUN Rscript -e "remotes::install_cran('data.table')"
RUN Rscript -e "remotes::install_cran('dplyr')"
RUN Rscript -e "remotes::install_cran('forestplot')"
RUN Rscript -e "remotes::install_cran('splines')"
RUN Rscript -e "remotes::install_cran('tidyr')"
RUN Rscript -e "remotes::install_cran('stringr')"
RUN Rscript -e "remotes::install_cran('survival')"
RUN Rscript -e "remotes::install_cran('codetools')"
RUN Rscript -e "remotes::install_cran('glmnet')"
RUN Rscript -e "remotes::install_cran('glmpath')"
RUN Rscript -e "remotes::install_cran('testthat')"
RUN Rscript -e "remotes::install_cran('DBI')"
RUN Rscript -e "remotes::install_cran('odbc')"

# -- vanilla because there is a bug that causes the R intro / preamble text to get pushed into the compiler
RUN Rscript --vanilla -e "remotes::install_cran('lme4', repos='"$R_REPOSITORY"')"
RUN Rscript --vanilla -e "remotes::install_cran('survminer', repos='"$R_REPOSITORY"')"

# install R packages for connecting to SQL Server and working with resulting data sets
RUN Rscript -e "devtools::install_github('https://github.com/nathan-palmer/FactToCube.git', ref='v1.0.0')"
RUN Rscript -e "devtools::install_github('https://github.com/nathan-palmer/MsSqlTools.git', ref='v1.0.0')"
RUN Rscript -e "devtools::install_github('https://github.com/nathan-palmer/SqlTools.git', ref='v1.0.0')"

# additional packages for NHANES analyses
RUN Rscript -e "remotes::install_cran('broom')"
RUN Rscript -e "remotes::install_cran('Hmisc')"
RUN Rscript -e "remotes::install_cran('nonnest2')"
RUN Rscript -e "remotes::install_cran('reshape2')"
RUN Rscript -e "remotes::install_cran('jsonlite')"
RUN Rscript -e "remotes::install_cran('viridis')"
RUN Rscript -e "remotes::install_cran('optparse')"

# need old version of rvest in order for the hack that parses URLs to work in the NHANES download script
RUN Rscript -e "remove.packages('rvest')"
RUN Rscript -e "remotes::install_cran('rvest', repos='https://cran.microsoft.com/snapshot/2021-01-29')"

# allow modification of these locations so users can install R packages without warnings
RUN chmod -R 777 /usr/local/lib/R/library
RUN chmod -R 777 /usr/local/lib/R/doc/html/packages.html


#------------------------------------------------------------------------------
# Install and configure RStudio Server
#------------------------------------------------------------------------------

RUN mkdir /opt/rstudioserver
WORKDIR /opt/rstudioserver

RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl1.0/libssl1.0.0_1.0.2n-1ubuntu5_amd64.deb
RUN dpkg -i ./libssl1.0.0_1.0.2n-1ubuntu5_amd64.deb

RUN apt-get update && apt-get install -y gdebi-core

# older RStudio version (try to deal with name / pwd prompt from git credential manager):
# 1.2 works, later versions require modifying the GIT_ASKPASS environment variable
# to suppress a prompt in R
# RUN wget https://download2.rstudio.org/server/trusty/amd64/rstudio-server-1.2.5042-amd64.deb
# RUN gdebi -n rstudio-server-1.2.5042-amd64.deb

RUN wget https://download2.rstudio.org/server/bionic/amd64/rstudio-server-2022.07.2-576-amd64.deb
RUN gdebi --non-interactive rstudio-server-2022.07.2-576-amd64.deb

# Copy RStudio Config
COPY Container/rserver.conf /etc/rstudio/rserver.conf


#------------------------------------------------------------------------------
# Final odds and ends
#------------------------------------------------------------------------------

# Copy startup script
RUN mkdir /startup
COPY Container/startup.sh /startup/startup.sh
RUN chmod 700 /startup/startup.sh

# Create a mount point for host filesystem data
RUN mkdir /HostData

# Copy the download and demo scripts
RUN mkdir /NHANES
COPY Code/R/download.R /NHANES/download.R
COPY Code/R/regressionExample.R /NHANES/regressionExample.R
RUN chmod -R 777 /NHANES

RUN mkdir /NHANES/Data
RUN chmod -R 777 /NHANES/Data

ARG ACCEPT_EULA=Y
ARG SA_PASSWORD=yourStrong(!)Password

ARG COLLECTION_DATE=4-14-2023
ARG EPICONDUCTOR_CONTAINER_VERSION=1.0.0

ENV EPICONDUCTOR_CONTAINER_VERSION=$EPICONDUCTOR_CONTAINER_VERSION 
ENV COLLECTION_DATE=$COLLECTION_DATE

# run the download and database build
RUN 

RUN runuser -m -p  mssql -c '/opt/mssql/bin/sqlservr --accept-eula & (echo "awaiting server bootup" && sleep 15 && echo "lets try to logon"  && /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SA_PASSWORD})' \
	&& Rscript /NHANES/download.R --container-build=TRUE
	
# # run the ETL process to load variable metadata
COPY Code/R/etlVariableCodebook.R /NHANES/etlVariableCodebook.R
COPY Code/R/createQuestionnaireViews.R /NHANES/createQuestionnaireViews.R
WORKDIR /tmp
RUN git clone https://github.com/ccb-hms/NHANES-metadata.git
WORKDIR /tmp/NHANES-metadata
RUN git fetch --tags
RUN latestTag=$(git describe --tags `git rev-list --tags --max-count=1`)
RUN git checkout $latestTag


RUN python3 -m pip install pandas
RUN python3 -m pip install Owlready2
RUN python3 -m pip install text2term
RUN python3 code/preprocess_metadata.py
RUN python3 code/generate_ontology_mappings.py

RUN runuser -m -p  mssql -c '/opt/mssql/bin/sqlservr &' \
	&& Rscript /NHANES/etlVariableCodebook.R

# install packages, which require newer version of rvest in conflict with the download / build script
RUN Rscript -e "remotes::install_version('nhanesA', repos='https://cran.microsoft.com/snapshot/2022-11-06', upgrade=TRUE, version='0.7.1')"
RUN Rscript -e "remotes::install_version('DT', repos='https://cran.microsoft.com/snapshot/2022-11-06', upgrade=TRUE, version='0.26')"

CMD ["/startup/startup.sh"]
