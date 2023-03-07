import argparse
import os
import shutil
import sys
import datetime
import pandas as pd
from sql_connect import sql_connect as sql
from constants import constants

# Algoritmo temporal de obtención de archivos del datalake

#ToDo:
## Flexibilizar para poder filtar por más metadatos
## Meter a log todo!!!!!! Y volcarlo en la carpeta del job
## Meter esto en path

def arg_parser():
    parser = argparse.ArgumentParser(description="Herramienta temporal de obtención de archivos fastq del data lake")
    parser.add_argument('--sample', help = 'Se obtienen las secuencias con el número de muestre indicado. Pueden indicarse varias, separadas por , sin espacio.')
    #parser.add_argument('--csv', help='CSV con lista de muestras')
    parser.add_argument('--copy-files', help= 'Genera una copia del archivo en lugar de un enlace simbólico', action='store_true')
    parser.add_argument('--prefix', help='Prefijo para la carpeta de trabajo', default= '')
    args = parser.parse_args()
    return args

# Sql connection

def get_table(table_name, engine):
    try:
        datalake_bd = pd.read_sql(table_name, engine)
        return datalake_bd
    except Exception as e:
        print('Error en la consulta de pandas')
        print(e)
        exit()
    
def create_instr(args):
    if args.copy_files:
        instr = shutil.copy
    else:
        instr = os.symlink
    return instr

def manage_duplicates(df, col, sample_list):
    paths_dict = {}
    paths = df[df[col].isin(sample_list)]
    for sample in sample_list:
        paths_dict[sample] = []
        subset = paths[paths[col] == sample].reset_index(drop=True).copy()
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
    return paths_dict

def create_job (job_tag, date, cwd, prefix = ""):
    job_folder = f'{cwd}/{prefix}{job_tag}{date}'
    if not os.path.exists(job_folder):
        os.mkdir(job_folder)
    return job_folder

def main():
    
    #args
    
    args = arg_parser()
    
    # constants
    cwd = os.getcwd()

    sql_constants = constants.sql_constants

    date_constants = constants.date_formats

    prefix_constants = constants.prefix

    cdate = datetime.datetime.now().strftime(date_constants.job_format)

    engine = sql.SQL_connect(sql_constants.lims_schema)[1]

    #instruction

    instr = create_instr(args)

    #get table

    datalake_bd = get_table(sql_constants.data_lake_table, engine)
    #sample list

    if args.sample:
        query = [str(sample).strip() for sample in args.sample.split(',')]

    no_paths = [sample for sample in query if sample not in pd.Series(datalake_bd[sql_constants.sample_col]).to_list()]
    samples = [sample for sample in query if sample not in no_paths]

    #dupllicates

    paths_dict = manage_duplicates(datalake_bd, sql_constants.sample_col, samples)


    # Create job and recover fastq

    job_folder = create_job(prefix_constants.job_prefix, cdate, cwd, args.prefix)

    for sample, files in paths_dict.items():
        for idx, file in enumerate(files):
            if len(files)>1:
                instr(file, f"{job_folder}/{sample}-{idx}.fastq")
            else:
                instr(file, f"{job_folder}/{sample}.fastq")
    if no_paths:
        print(f'La(s) muestra(s) {" ".join(no_paths)} no se han encontrado')


if __name__ == "__main__":
    main()