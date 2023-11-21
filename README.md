# NHANES

This repository contains R code and a Docker image definition that facilitate pulling the CDC's NHANES (SAS) data files into text and / or inserting into SQL Server tables.

The image contains SQL Server for Linux, R, and RStudio Server, among other utilities.

## Code
The [code](https://github.com/ccb-hms/NHANES/tree/main/Code) folder contain R and SQL scripts used to download and ETL the CDC data. 
* `download.R` Downloads the CDC SAS files directly from the CDC NHANES website.
* `etlVariableCodebook.R` Loads the NHANES variable codebooks available [here: ](https://github.com/ccb-hms/NHANES-metadata.git)
* `translateRawTables.R` Translates the variable responses in each NHANES questionnaire table.

## Container
The [container](https://github.com/ccb-hms/NHANES/tree/main/Container) folder contains the Dockerfile and related startup script.
* `Dockerfile` is the container image for CCB's NHANES database project

## Testing/Code
The [Testing/Code](https://github.com/ccb-hms/NHANES/tree/main/Testing/Code) folder contains the testing script that runs at the end of the build to verify database structure, row count agreements, and general structural consistency between versions.
* `containerBuildTests.R` contains all tests completed at build time.

## Running the image
An image with the current pre-built database can be run as follows:

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
        hmsccb/nhanes-workbench:version-0.2.0
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
