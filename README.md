En este repositorio se encontaran 3 carpetas y el archivo yaml, en la carpeta dags hay dos archivos.
El primero file_upload.py se encarga de borrar, los datos dentro de la tabla penguins (dentro de la base de datos Postgres) y luego lee los datos que estan en la tabla penguins_lter_pipe.csv (carpeta "data") y los sube a la tabla penguins nuevamente.
El segundo archivo DAG file_transformation_modelo.py, se encargara de leer los datos de la tabla penguins, los limpiara y transformara y ajustara un modelo. El resultado de este DAG son los archivos que quedan en la carpeta results ( pickle modelo, Model_results y por ultimo la tabla cruzada de los datos test)

El archivo docker-compose.yaml, levanta los contenedores necesarios para poder trabajar con apache Airflow y una base de datos postgresSQL.


Autores

Daniel Cordoba Pulido
Daniel de Jesus Martinez
