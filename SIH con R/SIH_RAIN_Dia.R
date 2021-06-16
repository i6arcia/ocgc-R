#### Genera los archivos de precipitación y temperaturas desde el SIH
#### Elimana datos superiores a los limites históricos
#### Elabora los boxplot por día por región
#### Actualización Diaria de los archivos
#### Elaboró LCA Carlos R. Garrido Díaz
############################## DATOS PRECIPITACIÓN ##############################
library("RColorBrewer")
library(RMySQL)

#Datos.Ext<-read.table("Datos/Extremos_NODO.csv", sep=",", header=T, as.is=T)
#Datos.Ext.EDO<-read.table("Datos/Extremos_EDO.csv", sep=",", header=T, as.is=T)
#REGS<-unique(Datos.Ext$Nodo)
#REGS.Edo<-unique(Datos.Ext.EDO$Edo)

#Hoy<-Sys.Date()
#Hoy<-as.Date(scan("~/SCRIPT/Fecha.txt",what="character"))
Hoy <- as.Date("2021-05-18")

#con <- dbConnect( dbDriver("MySQL"), user="ccc", dbname="sih", host="172.29.69.226")
con <- dbConnect( dbDriver("MySQL"), user="ccc", dbname="sih", host="172.29.12.4")
print(paste("Dia:", Hoy))
Vfecha<-format.Date(Hoy, "X%Y.%m.%d")
RAIN<-read.table(paste("Datos/RAIN", format.Date(Hoy, "%Y.csv"), sep=""), sep=",", header=T, as.is=T,na.strings="-99.9")
#RAINNV<-read.table(paste("Datos/RAIN", format.Date(Hoy, "%Y_NV.csv"), sep=""), sep=",", header=T, as.is=T,na.strings="-99.9")

#Mes<-format.Date(Hoy, format="%B")
#Datos.Ext.1<-Datos.Ext[Datos.Ext$Mes==Mes,]
#Datos.Ext.Edo.1<-Datos.Ext.EDO[Datos.Ext.EDO$Mes==Mes,]

RainSIH<-dbGetQuery(con, paste("SELECT * FROM ddprecipitacio WHERE Datee=", "'", Hoy, "'", sep="" ) )
#Rain233<-dbGetQuery(con2, paste("SELECT * FROM ddprecipitacio WHERE Datee=", "'", Dia, "'", sep="" ) )
RainSIH <- RainSIH[c("Station","Valuee")]
#Rain233 <- Rain233[c("Station","Valuee")]

#Rain.1<-data.frame(Est=c(RainSIH$Station, Rain233$Station), Rain=c(RainSIH$Valuee, Rain233$Valuee) )
#Rain.1<-data.frame(Est=c(RainSIH$Station), Rain=c(RainSIH$Valuee) )
RainSIH <-na.omit(unique(RainSIH))
RainSIH$Valuee[RainSIH$Valuee<0]<-RainSIH$Valuee[RainSIH$Valuee<0]*-1
for(loop.2 in RAIN$EST){
  if( nrow(RainSIH[RainSIH$Station==loop.2, ])==1 & is.na(RAIN[RAIN$EST==loop.2, Vfecha]) ) RAIN[RAIN$EST==loop.2, Vfecha] <- RainSIH[RainSIH$Station==loop.2, "Valuee"]
  if( nrow(RainSIH[RainSIH$Station==loop.2, ])==1) RAINNV[RAINNV$EST==loop.2, Vfecha] <- RainSIH[RainSIH$Station==loop.2, "Valuee"]
  
  #if( nrow(Rain.2[Rain.2$Est==loop.2, ])==2) {RAIN[RAIN$EST==loop.2, fecha] <- Rain.2[Rain.2$Est==loop.2, "Rain"][1]; print(Rain.2[Rain.2$Est==loop.2, "Rain"] ) }
  }#loop.2

write.table(RAIN,format.Date(Hoy, format="Datos/RAIN%Y.csv"), sep=",", row.names=F, col.names=T, na="-99.9")
write.table(RAINNV,format.Date(Hoy, format="Datos/RAIN%Y_NV.csv"), sep=",", row.names=F, col.names=T, na="-99.9")
dbDisconnect(con)
print("Terminado")

    dbListConnections(MySQL())
    