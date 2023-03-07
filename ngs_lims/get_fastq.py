import argparse
import os
import shutil
import sys
import datetime
import pandas as pd
import pymysql
import getpass
from sqlalchemy import create_engine

#ToDo:
## Flexibilizar para poder filtar por más metadatos
## Meter a log todo!!!!!! Y volcarlo en la carpeta del job
## Meter esto en path
## Empaquetar las funciones
def SQL_connect(schema, host = 'localhost', port = 3306):
    print('Conectando MySQL')
    print('Introduce el usuario:')
    user = input()
    print('Introduce la contraseña')
    password = getpass.getpass()
    try:
        conexion = pymysql.connect(host=host,
                        user=user,
                        password=password,
                        db= schema,
                        port = port
        )
        print('Conexion establecida!')
    except Exception as e:
        print("No se ha podido establecer la conexión con MySQL. El proceso se ha abortado")
        print(e)
        exit()
    db_data = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    engine = create_engine(db_data, encoding='latin1')
    cursor = conexion.cursor()
    return (conexion, engine, cursor)

parser = argparse.ArgumentParser(description="Herramienta temporal de obtención de archivos fastq del data lake")
parser.add_argument('--sample', help = 'Se obtienen las secuencias con el número de muestre indicado. Pueden indicarse varias, separadas por , sin espacio.')
#parser.add_argument('--csv', help='CSV con lista de muestras')
parser.add_argument('--copy-files', help= 'Genera una copia del archivo en lugar de un enlace simbólico', action='store_true')
parser.add_argument('--prefix', help='Prefijo para la carpeta de trabajo')
args = parser.parse_args()

sample_col = 'sample_name'
cdate = datetime.datetime.now().strftime("%Y_%m_%d-%H-%M-%S")
cwd = os.getcwd()

schema = 'NGS_LIMS'

# Sql connection

conexion, engine, cursor = SQL_connect(schema)

try:
    datalake_bd = pd.read_sql("data_lake", engine)
except Exception as e:
    print('No se puede abrir la base de datos!')
    print(e)
    exit()

if args.sample:
    query = [str(sample).strip() for sample in args.sample.split(',')]

no_paths = [sample for sample in query if sample not in pd.Series(datalake_bd[sample_col]).to_list()]
samples = [sample for sample in query if sample not in no_paths]

#Obtenemos los paths y buscamos duplicados

paths_dict = {}
paths = datalake_bd[datalake_bd[sample_col].isin(samples)]
for sample in samples:
    paths_dict[sample] = []
    subset = paths[paths[sample_col] == sample].reset_index(drop=True).copy()
    if subset.shape[0]>1:
        breaker = True
        while breaker:
            breaker = False
            print(f"La muestra {sample} tiene varios archivos")
            for idx, row in subset.iterrows():
                print(f"{idx} : {row['file']}")
            print('Indica los índices seleccionados separados por comas. Ej: 1,3')
            print('Si quieres todos los archivos, escribe all. Si no quieres ninguno, escribe any. Sal con exit')
            input_idx = input()
            if input_idx == 'exit':
                sys.exit()
            elif input_idx == 'all':
                idxes = [idx for idx in subset.index]
            elif input_idx == 'any':
                idxes = []
            else:
                idxes = [int(n) for n in input_idx.split(',')]
                checker = [idx for idx in idxes if idx not in subset.index]
                if checker:
                    print('Indice(s) incorrecto(s):')
                    print(checker)
                    breaker = True
        if idxes:
            print(subset)
            paths_dict[sample] += [subset.loc[subset.index[idx], 'path'] for idx in idxes]
    else:
        paths_dict[sample]+= [subset.loc[subset.index[0], 'path']]

# Assign instruction

if args.copy_files:
    instr = shutil.copy
else:
    instr = os.symlink

# Create job and recover fastq

job_folder = f'{cwd}/{args.prefix}-job_{cdate}'
if not os.path.exists(job_folder):
    os.mkdir(job_folder)

for sample, files in paths_dict.items():
    for idx, file in enumerate(files):
        if len(files)>1:
            instr(file, f"{job_folder}/{sample}-{idx}.fastq")
        else:
            instr(file, f"{job_folder}/{sample}.fastq")

# No paths
if no_paths:
    print(f'La(s) muestra(s) {" ".join(no_paths)} no se han encontrado')