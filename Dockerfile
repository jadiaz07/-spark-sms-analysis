FROM jupyter/pyspark-notebook:latest

# Instalar dependencias adicionales
RUN pip install matplotlib pandas

# Copiar archivos al contenedor
COPY eventos.csv.gz /app/eventos.csv.gz
COPY free_sms_destinations.csv.gz /app/free_sms_destinations.csv.gz
COPY script.py /app/script.py

# Definir el directorio de trabajo
WORKDIR /app

# Ejecutar el script automáticamente al iniciar el contenedor
CMD ["spark-submit", "script.py"]