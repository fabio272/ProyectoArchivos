ARCHIVO DE CONFIGURACION: 
Se compone por dos campos con nombres CLAVE y VALOR separados por un caracter pipe(|).
Los valores de las variables pueden quedar en blanco salvo los señalados a continuación con un asterisco (*):

sourcePath -> Directorio con el archivo origen, inclusive el mismo. (*)
finalPath -> Directorio destino. (*)
sep -> Separador en forma de caracter.
header -> Header del archivo a replicar, con dos opciones unicamente (True - False). (*)
sepVariable -> Separador de múltiples valores de variables en el archivo Config. (*)
sepArg -> Separador de múltiples valores de argumentos pasados a la ejecución. (*)
fwf -> Identidica si el archivo es de ancho fijo, con dos opciones unicamente (TRUE - FALSE). (*)
fwfLenghts -> Valores de longitud de campos de archivo de ancho fijo. Pueden ser múltiples y separados por el valor otorgado a la anterior variable "sepVariable" sin espacios.
columnNames -> Nombre de campos de archivo de ancho fijo. Pueden ser múltiples y separados por el valor otorgado a la anterior variable "sepVariable" sin espacios.
timestampType -> Todos los campos de tipo timestamp a castear. Pueden ser múltiples y separados por el valor otorgado a la anterior variable "sepVariable" sin espacios.
timestampFormat -> Formato de los campos tipo timestamp a castear.
dateType -> Todos los campos de tipo date a castear. Pueden ser múltiples y separados por el valor otorgado a la anterior variable "sepVariable" sin espacios.
dateFormat -> Formato de los campos tipo date a castear.
floatType -> Todos los campos de tipo float a castear. Pueden ser múltiples y separados por el valor otorgado a la anterior variable "sepVariable" sin espacios.
intType -> Todos los campos de tipo int a castear. Pueden ser múltiples y separados por el valor otorgado a la anterior variable "sepVariable" sin espacios.
writeFormat -> Formato en que se escribe el archivo en el destino (parquet, avro, etc.). (*)
writeMode -> Modo de escritura del archivo (Overwrite - Append). (*) 
partition -> Nombre del campo partición. Solo admite uno y puede ser un campo nuevo pasado por argumentos (Los mismos deben coincidir en nombre). Si se configura con valor "default" aplica el siguiente campo.
defaultPartition -> Nombre del campo de partición por defecto. la variable "partition" tiene que estar configurada de la siguiente forma: partition|default
typePartition -> Tipo del campo partición. Solo admite tres (date,int,str).

MODOS DE EJECUCIÓN:

El programa admite parametros, un máximo de tres, los cuales son:
	- Path de archivo de configuración, con el nombre del archivo inclusive.
	- Lista de campos nuevos a añadir. Separados por el mismo caracter configurado en "sepArg" sin espacios.
	- Lista de valores de los campos nuevos a añadir. Separados por el mismo caracter configurado en "sepArg" sin espacios (admite el valor "current_date" para la fecha actual en formato int = yyyyMMdd o formato date = yyyy-MM-dd. Determinado por "typePartition").

Ejemplos de ejecuciones:
	- Sin campos nuevos: %run ProyectoArchivos.py
	- Con campos nuevos: %run ProyectoArchivos.py Campo1;Campo2 Valor1;current_date
	
RESPUESTA PREGUNTA SOBRE OVERWRITE EN SPARK:
	En las versiones de Spark anteriores a 2.3.0, las particiones no eran dinamicas, las mismas tenian que apuntarse a cada folder de partición con valor correspondiente o inclusive antes de eso se tenia que detener la creación de metadata y eliminar el archivo de _SUCCES.
	Ya en la versión 2.3.0+ se añadieron las opciones en modo dinamico o estatico y las particiones se sobre escriben unicamente a las que correspondan.