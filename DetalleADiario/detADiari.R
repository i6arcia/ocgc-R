#Actualiza en la base de datos la informacion de lluvia de la tabla datos Detalle a la tabla datos Diarios
#09/06/2021
#Desarrollado: Irvin de Jesús García Apodaca

library(DBI)
library(RMySQL)


print ("Iniciando programa")

#Grupo de estaciones para trabajar
grp = 'autgcn'

#Variables de fecha
fch = Sys.Date()       #Variable fecha actual
#fch = as.Date("2021-06-04")       #Variable fecha actual
fchIni <- format.Date(fch-1,"%Y-%m-%d 08:00")   #Variable de inicio para consultar un grupo de fechas
fchFin <- format.Date(fch,"%Y-%m-%d 08:00")   #Variable de fin para consultar un grupo de fechas

#Conexión a la base de datos
sihDb <- dbConnect(dbDriver("MySQL"), user="ccc", dbname="sih", host="172.29.12.4")

#Obtiene claves del grupo de estaciones 
query <- paste("Select station from stationgroups where stationgroup = '", grp ,"'", sep = "")
estaciones <- dbGetQuery(sihDb, query)

for(st in estaciones$station){
  query <- paste("select sum(valuee) as sum from dtprecipitacio where station = '", st ,"' and datee >= '", fchIni ,"' and datee < '", fchFin ,"'", sep = "")
  #Ejecuta la consulta en la base de datos
  acum <- dbGetQuery(sihDb,query[1])
  if (!is.na(acum)){
    #print(acum)
    query <- paste("Replace into ddPrecipitacio (Station, Datee, Valuee, msgCode, acumValue, numdia, cantEstac) Values ('", st, "', '", fch, "', '", round(acum$sum, 1), "', '', '0', '9', '0')", sep = "")
    #print (query)
    dbGetQuery(sihDb, query)
  }
}

#Cierra todas las conexiones a la base de datos
for (x in dbListConnections(MySQL())){
  dbDisconnect(x)
}

dbListConnections(MySQL())
print("Programa terminado!")
