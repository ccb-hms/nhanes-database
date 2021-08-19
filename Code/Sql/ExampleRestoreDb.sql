USE [master]
RESTORE DATABASE [NhanesLandingZone] FROM  DISK = N'S:\UserTemp\NhanesLandingZone.bak' WITH  FILE = 1,  MOVE N'NhanesLandingZone' TO N'F:\MSSQL14.MSSQLSERVER\MSSQL\USER_DB/NhanesLandingZone.mdf',  MOVE N'NhanesLandingZone_log' TO N'F:\MSSQL14.MSSQLSERVER\MSSQL\USER_DB/NhanesLandingZone_log.ldf',  NOUNLOAD,  STATS = 5
