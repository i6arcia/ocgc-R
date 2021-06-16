#01 - junio - 2021
#Irvin de Jesús García Apodaca
#Pruebas de conexión a la base de datos

library(DBI)
library(RMySQL)
#Grupo de estaciones a consultar
grp = 'GRGCTODO'

#Variables de fecha
fch = as.Date("2020-01-31")       #Variable para consultar un solo día
fchIni <- as.Date("2021-06-10")   #Variable de inicio para consultar un grupo de fechas
fchFin <- as.Date("2021-06-20")   #Variable de fin para consultar un grupo de fechas

#Vectores de fechas
fechas <- seq(fchIni,fchFin, "day")          #Crea una lista de fechas con los rangos establecidos FchIni: FchFin
fechasX <- format.Date(fechas, "X%Y.%m.%d")  #Crea una lista de fechas con la estructura X mas YYYY.MM.DD
fchX <- format.Date(fch, "X%Y.%m.%d")        #Almacena la variable fecha con formato X mas YYYY.MM.DD

#Importa archivo de excel
xlsDatos <- read.table("Datos/DatosLluvia.csv", sep = ",", header = T, as.is = T, na.strings = "-99.9")

#Escribe los encabezados fechas en el archivo

if (names(xlsDatos[16]) != format.Date(fchFin,"X%Y.01.01")){
  print("Las fechas cambiaron")
  f1 <- as.Date(format(fchIni, "%Y-01-01"))
  f2 <- as.Date(format(fchIni, "%Y-12-31"))
  fx <- seq(f1,f2+1, "day")
  fx <- format.Date(fx, "X%Y.%m.%d")
  encabezados <- c("Station", "StationName", "municipio", "nommunicipio", "estado", "cuenca", "nomcuenca", "Clicom", "Longitud", "Latitud", "Altitud", "YLA", "XLO", "ccl2latitud", "ccl2longitud")
  encabezados <- c(encabezados,fx)
  
  names(xlsDatos)= encabezados
  
  for (i in 16:ncol(xlsDatos)){
    xlsDatos[,as.integer(i)] <- -99.9
  }
  #Guarda los resultados en el archivo CSV
  write.table(xlsDatos,"Datos/DatosLluvia.csv", sep=",", row.names=F, col.names=T, na="-99.9")
  #Importa archivo de excel de nuevo
  xlsDatos <- read.table("Datos/DatosLluvia.csv", sep = ",", header = T, as.is = T, na.strings = "-99.9")
}else print("Se mantiene las fechas")

#nrow(tblDatos[tblDatos$Station == "CHIVC",])


#Conexión a la base de datos
sihDb <- dbConnect(dbDriver("MySQL"), user="ccc", dbname="sih", host="172.29.12.4")

#Ciclo para recorrer las fechas
for (i in 0:length(fechas)) {
  print (paste("Consultando datos de fecha: ", fechas[i], sep = ""))
  #print (paste("Consultando datos de fecha: ", fch, sep = ""))
  
  query <- paste("Select t1.Station, t1.Valuee from ddPrecipitacio t1, stationgroups t2 where Datee = '", fechas[i] , "' and t2.stationgroup = '", grp,"' and t1.station = t2.station", sep = "")
  #query <- paste("Select t1.Station, t1.Valuee from ddPrecipitacio t1, stationgroups t2 where Datee = '", fch , "' and t2.stationgroup = '", grp,"' and t1.station = t2.station", sep = "")
  query
  #Ejecuta la consulta en la base de datos
  tblDatos <- dbGetQuery(sihDb,query)
  #Muestra el numero de resultados de la consulta
  nrow(tblDatos)
  #Recorre los resultados para agregarlos en la tabla xlsDatos
  for(j in tblDatos$Station){
    #Compara la coincidencia entre claves de estación
    if (nrow(xlsDatos[xlsDatos$Station == j, ]) > 0) {
      #print("La estación si está en la tabla")
      #Guarda el valor de la consulta en la tabla xlsDatos
      xlsDatos[xlsDatos$Station == j,fechasX[i]] <- tblDatos[tblDatos$Station== j,2]
    }else{
      print(paste("La estación no está en la tabla ", j, sep = ""))
    }
  }
}

#Guarda los resultados en el archivo CSV
write.table(xlsDatos,"Datos/DatosLluvia.csv", sep=",", row.names=F, col.names=T, na="-99.9")

#Cierra todas las conexiones a la base de datos
for (x in dbListConnections(MySQL())){
  dbDisconnect(x)
}

dbListConnections(MySQL())
print("Programa terminado!")
