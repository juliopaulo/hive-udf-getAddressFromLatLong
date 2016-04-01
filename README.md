# hive-udf-getAddressFromLatLong


This example resolved the problem displayed in cloudera CPP Data Engineer

"Convert data from one set of values to another (e.g., Lat/Long to Postal Address using an external library)"

# Step 1
Execute this command in hive console

add  jar /home/cloudera/.m2/repository/com/hive/udf/hive-udf-address/1.0-SNAPSHOT/hive-udf-address-1.0-SNAPSHOT.jar;

# Step 2
Execute this command in hive console

CREATE TEMPORARY FUNCTION getaddress AS 'com.hive.udf.GetAddressFromGeo';

# Step 3
Execute this command in hive console

select getaddress(latitud,longitud) from address limit 5 ;


Result here !,


 "Passatge de Conradí, 333I, 08025 Barcelona, Barcelona, Spain",
 : "Carrer d'Aragó, 39, 08015 Barcelona, Barcelona, Spain",
 : "Carrer de Lepant, 294, 08025 Barcelona, Barcelona, Spain",
 : "Carrer de Mallorca, 26, 08014 Barcelona, Barcelona, Spain",
 : "San Joaquin 400, Santa Cruz de la Sierra, Bolivia",
 : "San Joaquin 400, Santa Cruz de la Sierra, Bolivia",
no address
 : "Potrero Chico 333, Villa los Portales, 66120 Cd Santa Catarina, N.L., Mexico",
 : "Plaça de Cort, 1, 07001 Palma, Illes Balears, Spain",
no address
no address
 : "Manuel Maria Caballero 3105, Santa Cruz de la Sierra, Bolivia",
