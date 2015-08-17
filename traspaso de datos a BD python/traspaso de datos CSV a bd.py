# -*- coding: utf-8 -*-
import csv
import codecs
import sqlite3 as sqlite
import os.path

print "procesar archivos tipo csv de reclamos del SERNAC 2010-2011"

nombre_BD="reclamos.db"

#conectarse y o crear base de datos
existe_bd = os.path.exists(nombre_BD) #verificar si db existe
connection = sqlite.connect(nombre_BD)

try:
	if not existe_bd:
		print "No existe la base de datos "+nombre_BD
		#raw_input()
		cursor = connection.cursor()
		cursor.execute("CREATE TABLE registro_reclamo (id INTEGER PRIMARY KEY AUTOINCREMENT,año INTEGER,comuna VARCHAR,region VARCHAR,mercado VARCHAR, problema VARCHAR);")
			

except Exception as e:
 print "ha ocurrido un error al crear la tabla: "+format(e)	
cursor = connection.cursor()

file= "Reclamos_2010_2011_DataSet.csv"
reg=0 #contados de registros almacenados
lim=1000 #variable de control de informacion

openf=codecs.open(file,"r",encoding='utf-8') #se crea un manejador de archivos.
open_csv=csv.reader(openf, delimiter=';') #manejador de archivos CSV.

registros=[] #arreglo para almacenar contenido del archivo csv

for l in open_csv:
		registros.append(l)


try:
	for l in registros:
		cursor.execute("insert into registro_reclamo(año,comuna,region,mercado,problema) values(?,?,?,?,?)", (l[0],l[1],l[3],l[4],l[8]))
	
		connection.commit() #enviar transaccion a la base de datos.
		
		reg=reg+1
		if reg==lim:
			print "van "+str(reg)+" datos procesados."
			lim=lim+1000
		
except Exception as a:
 print "ha ocurrido un error en el 2do ciclo for: "+format(a)
#raw_input()
print "numero de registros almacenados:"+str(reg)


raw_input()

openf.close() #cierra el archivo.
