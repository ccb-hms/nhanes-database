
# NHANES :apple:

A Dockerized approach to extracting, transforming, loading, and querying the CDC's NHANES data into SQL Server tables.

The image contains SQL Server for Linux, R, and RStudio Server, among other utilities.

## Requirements 

Docker
- [Instructions for *Windows*](https://docs.docker.com/desktop/install/windows-install/)
- [Instructions for *Mac with Apple Silicon*](https://desktop.docker.com/mac/main/arm64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=docs-driven-download-mac-arm64&_gl=1*1my1z5l*_ga*MTQ2NjYyNTU2NS4xNzEwNDM0MTQ4*_ga_XJWPQMJYHQ*MTcxMDQzNDE0OC4xLjEuMTcxMDQzNDE0OS41OS4wLjA.)
- [Instructions for *Mac with Intel chip*](https://desktop.docker.com/mac/main/amd64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=docs-driven-download-mac-amd64&_gl=1*xd49cn*_ga*MTQ2NjYyNTU2NS4xNzEwNDM0MTQ4*_ga_XJWPQMJYHQ*MTcxMDQzNDE0OC4xLjEuMTcxMDQzNDE0OS41OS4wLjA.)

A basic understanding of Docker is recommended, a beginners guide is offered through [Docker's startup guide.](https://docker-curriculum.com)

A SQL Server compatible data management / database viewing tool such as Azure Data Studio, SSMS, ODBC, JDBC is recommended.

## Directory Guide

### /Code
The [code](https://github.com/ccb-hms/NHANES/tree/main/Code) folder contain R and SQL scripts used to download and ETL the CDC data. 
* `download.R` Downloads the CDC SAS files directly from the CDC NHANES website.
* `etlVariableCodebook.R` Loads the NHANES variable codebooks available [here: ](https://github.com/ccb-hms/NHANES-metadata.git)
* `translateRawTables.R` Translates the variable responses in each NHANES questionnaire table.

### /Container
The [container](https://github.com/ccb-hms/NHANES/tree/main/Container) folder contains the Dockerfile and related startup script.
* `Dockerfile` is the container image for CCB's NHANES database project

### /Testing/Code
The [Testing/Code](https://github.com/ccb-hms/NHANES/tree/main/Testing/Code) folder contains the testing script that runs at the end of the build to verify database structure, row count agreements, and general structural consistency between versions.
* `containerBuildTests.R` contains all tests completed at build time.


## Running the image

### An image with the current pre-built database can be run as follows:

#### MacOS

```
docker \
    run \
        --rm \
        --platform=linux/amd64 \
        --name nhanes-workbench \
        -d \
        -v LOCAL_DIRECTORY:/HostData \
        -p 8787:8787 \
        -p 2200:22 \
        -p 1433:1433 \
        -e 'CONTAINER_USER_USERNAME=USER' \
        -e 'CONTAINER_USER_PASSWORD=PASSWORD' \
        -e 'ACCEPT_EULA=Y' \
        -e 'SA_PASSWORD=yourStrong(!)Password' \
        hmsccb/nhanes-workbench:version-0.4.1
```

#### Windows

```
docker run --rm --platform=linux/amd64 --name nhanes-workbench  -d -v LOCAL_DIRECTORY:/HostData -p 8787:8787 -p 2200:22 -p 1433:1433 -e 'CONTAINER_USER_USERNAME=USER' -e 'CONTAINER_USER_PASSWORD=PASSWORD' -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=yourStrong(!)Password' hmsccb/nhanes-workbench:version-0.4.1
```

For other versions, see the [Dockerhub repository](https://hub.docker.com/r/hmsccb/nhanes-workbench/tags) and use the desired tag.

### Parameters

`LOCAL_DIRECTORY` is a directory on the host that you would like mounted at /HostData in the container.  Can be omitted.

`CONTAINER_USER_USERNAME` is the name of the user in the container that will be created at runtime.  You can connect via `ssh` or `RStudio Server` with this user name.

`CONTAINER_USER_PASSWORD` is the password of the user in the container that will be created at runtime.  You can connect via `ssh` or `RStudio Server` with this password.

`ACCEPT_EULA` is required for SQL Server to successfully start

`SA_PASSWORD` is the password for the SQL Server `sa` account.  See [here](https://docs.microsoft.com/en-us/sql/relational-databases/security/password-policy?view=sql-server-ver15) for complexity requirements.

### Port Forwarding

These options control port forwarding from the container to the host:

```
        -p 8787:8787 \
        -p 2200:22 \
        -p 1433:1433 \
```

Port 8787 is used for access to the RStudio Server HTTP server.  Port 2200 on the host provides access to ssh server in the container.  Port 1433 provides access to SQL Server running in the container.

## Connecting to the container

Database connectivity is enabled on TCP port 1433 on the host. Any standard tools that work with SQL Server (Azure Data Studio, SSMS, ODBC, JDBC) can be aimed at this port on the host to work with the DBs in the container.

You can can ssh into the container, e.g.:

```
ssh USER@HOST_ADDRESS -p 2200 -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null
```

If you are running SSH on the same host where the container is running:

```
ssh USER@localhost -p 2200 -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null
```


## 🐛 Bugs & Issues
If you encounter any bugs or would like to suggest functionality, please contribute to our efforts by [opening an issue](https://github.com/ccb-hms/NHANES/issues).