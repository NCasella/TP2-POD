# TP2-POD
Para preparar el entorno del servidor y del cliente, primero se debe correr el siguiente comando en el root del repositorio:

``mvn install ``

Luego, los pasos a seguir son similares tanto para el servidor como para el cliente.
## Servidor



Al cambiar el working directory al directorio *server/target*, se debe descomprimir el archivo .tar.gz que se encuentra en el directorio server/target con:

``tar -xzf tpe2-g7-server-1.0-SNAPSHOT-bin.tar.gz ``

Luego se debera entrar a la carpeta *tpe2-g7-server-1.0-SNAPSHOT* generada anteriormente, en el que se encontrara el script **run-server.sh**, que se puede ejecutar para lograr tener la instancia del servidor corriendo con:

``sh run-node.sh``

## Cliente

Los pasos a seguir son similares, con la excepcion de que se debe encontrar en el directorio *client/target* y descomprimir el archivo .tar.gz con:

``tar -xzf tpe2-g7-client-1.0-SNAPSHOT-bin.tar.gz ``

Luego al entrar a la carpeta *tpe2-g7-client-1.0-SNAPSHOT* se encontraran los siguientes scripts que corren sus respectivos clientes:

- query1.sh
- query2.sh
- query3.sh
- query4.sh

### Argumentos 

Todos los scripts de cliente reciben los siguientes argumentos:

- -Daddresses: Las direcciones IP de los nodos separados por punto y coma.

- -DinPath: Path en el que se encuentran los archivos .csv de multas, infracciones y tickets. 

- -DoutPath: Path en el que se encontraran los archivos de salida.

- -Dcity: Indica el dataset. Puede ser NYC o CHI.

Dependiendo del cliente, pueden necesitar los siguientes parametros:
##### Query 3

- -Dn: Cota inferior de la multas reincidentes
- -Dfrom: Fecha de inicio del rango de busqueda
- -Dto: Fecha de fin del rango de busqueda

#### Query 4

- -Dn: Limite de infracciones encontradas 
- -Dagency: Tickets emitidos por la agencia especificada
