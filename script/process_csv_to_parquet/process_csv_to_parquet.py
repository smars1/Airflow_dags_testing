import sys
import boto3
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

# Obtener argumentos del job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])

# Si INPUT_PATH termina en /, buscar el primer archivo CSV
if args['INPUT_PATH'].endswith('/'):
    s3 = boto3.client('s3')
    path = args['INPUT_PATH'].replace("s3://", "")
    bucket = path.split("/")[0]
    prefix = "/".join(path.split("/")[1:])

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    csv_files = [obj['Key'] for obj in contents if obj['Key'].endswith('.csv')]

    if not csv_files:
        raise Exception("No se encontraron archivos CSV en el bucket/prefix especificado")

    first_file = csv_files[0]
    args['INPUT_PATH'] = f"s3://{bucket}/{first_file}"

# Debug
print(f"INPUT_PATH usado: {args['INPUT_PATH']}")
print(f"OUTPUT_PATH usado: {args['OUTPUT_PATH']}")

# Inicializar contexto
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer CSV desde S3
df = spark.read.option("header", "true").csv(args['INPUT_PATH'])

# Transformaciones
df = df.dropna()
df = df.select([col(c).alias(c.strip().lower().replace(" ", "_")) for c in df.columns])

#show df
df.show()
df.printSchema()

# Escribir en formato Parquet en S3
df.write.mode("overwrite").parquet(args['OUTPUT_PATH'])

# Finalizar job correctamente
job.commit()
