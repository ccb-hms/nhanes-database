# debug r/w performance issues
result = haven::read_xpt("/HostData/Debug/PAXMIN_G.XPT.txt")
gc()
result2 = haven::read_xpt("/HostData/Debug/PAXMIN_G.XPT.txt")
gc()
dfList = list(result, result2)

m = dplyr::bind_rows(dfList)
rm(dfList, result, result2)
gc()

write.table(
            m,
            file = paste(sep = "/", outputDirectory, currDataType),
            sep = "\t"
        )