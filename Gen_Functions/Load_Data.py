import sqlalchemy, os, sys, psycopg2
import pandas as pd
import sqlite3
from Gen_Functions.validations import val_max_column_value

# Credenciales de BD PCEnergy
from  Gen_Functions.util import decryp
conn_path_BD = os.path.abspath(os.path.dirname(sys.argv[0])) 
conn_BD = decryp(conn_path_BD[0:conn_path_BD.find('Ingeniero')]+r'Conexion_BD\connections.v01');

# Función para crear una tabla en la BD PCEnergy desde un DataFrame en Python
def create_table(DataFrame,TableName,Switch):
    # Creando la conexión
    engine = 'postgresql+psycopg2://%s:%s@%s:%s/%s'%(conn_BD.iloc[0]['USER'],
                                                     conn_BD.iloc[0]['PASSWORD'],
                                                     conn_BD.iloc[0]['SERVER'],
                                                     conn_BD.iloc[0]['PORT'],
                                                     conn_BD.iloc[0]['DB_NAME']
                                                     )
    conn = sqlalchemy.create_engine(engine);
    # Validando si se debe crear la tabla o actualizarla
    if (Switch==0):
        # Si el switche es 0 se debe crear la tabla nueva
        DataFrame.to_sql(TableName, conn, schema = 'pcenergy');
    else:
        # Si el switch es 1 se debe actualizar la tabla existente
        DataFrame.to_sql(TableName, conn, schema = 'pcenergy', if_exists='replace');
    conn.dispose()
    
# Funcion para realizar cualquier consulta/operación a la base de datos
def execute_query(script):
    query = str(script);
    engine = 'postgresql+psycopg2://%s:%s@%s:%s/%s'%(conn_BD.iloc[0]['USER'],
                                                     conn_BD.iloc[0]['PASSWORD'],
                                                     conn_BD.iloc[0]['SERVER'],
                                                     conn_BD.iloc[0]['PORT'],
                                                     conn_BD.iloc[0]['DB_NAME']
                                                     )    
    conn = sqlalchemy.create_engine(engine);
    conn.execute(query)
    conn.dispose();

# Funcion para realizar select's a cualquier tabla de PCEnergy
def PCEnergy(query):
    query = str(query)
    engine = 'postgresql+psycopg2://%s:%s@%s:%s/%s'%(conn_BD.iloc[0]['USER'],
                                                     conn_BD.iloc[0]['PASSWORD'],
                                                     conn_BD.iloc[0]['SERVER'],
                                                     conn_BD.iloc[0]['PORT'],
                                                     conn_BD.iloc[0]['DB_NAME']
                                                     )
    conn = sqlalchemy.create_engine(engine)
    data = pd.read_sql_query(query, conn)
    conn.dispose()
    return data

# Write data in BD PCEnergy
def write_table(df, table):
    engine = 'postgresql+psycopg2://%s:%s@%s:%s/%s'%(conn_BD.iloc[0]['USER'],
                                                     conn_BD.iloc[0]['PASSWORD'],
                                                     conn_BD.iloc[0]['SERVER'],
                                                     conn_BD.iloc[0]['PORT'],
                                                     conn_BD.iloc[0]['DB_NAME']
                                                     )
    con = sqlalchemy.create_engine(engine)
    df.to_sql("%s"%table, con=con, schema='%s'%(conn_BD.iloc[0]['SCHEMA']), if_exists="append", index=False, chunksize=int(len(df)/10000))
    con.dispose()

# Delete data in BD Aflorada
def delete_row(periodo, table):
    conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s"%(conn_BD.iloc[0]['DB_NAME'], 
                                                                               conn_BD.iloc[0]['USER'], 
                                                                               conn_BD.iloc[0]['PASSWORD'], 
                                                                               conn_BD.iloc[0]['SERVER'], 
                                                                               conn_BD.iloc[0]['PORT']
                                                                               ))
    query = """ DELETE FROM %s."%s" WHERE "PER_AFLO" = '%s' """%(conn_BD.iloc[0]['SCHEMA'], table, periodo)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# Funcion para cargar Direccion.txt especificando las columnas
#def direccion(columns=None):
#    path = r'\\gnfbaqcgep1\Planificacion\Desarrollos\BD\15_Direccion\Direccion.txt'
#    data_temp = pd.read_csv(path, sep='\t', encoding='CP1252', usecols=columns, low_memory=False)
#    if 'NIS_RAD' in data_temp.columns:
#        data_temp.drop_duplicates('NIS_RAD', keep='last', inplace=True)
#        data = data_temp.copy()
#    else:
#        data = data_temp.copy()
#    return data
#
## Funcion para cargar Circuitos.txt especificando las columnas
#def circuito(columns=None):
#    path = r'\\gnfbaqcgep1\Planificacion\Desarrollos\BD\16_Circuitos\Circuitos.txt'
#    data_temp = pd.read_csv(path, sep='\t', encoding='CP1252', usecols=columns, low_memory=False)
#    if 'NIS_RAD' in data_temp.columns:
#        data_temp.drop_duplicates('NIS_RAD', keep='last', inplace=True)
#        data = data_temp.copy()
#    else:
#        data = data_temp.copy()
#    return data
#
## Funcion para cargar Rutas_Itinerarios.txt especificando las columnas
#def ruta_itin(columns=None):
#    path = r'\\gnfbaqcgep1\Planificacion\Desarrollos\BD\04_Ruta_Itin\Rutas_Itinerarios.txt'
#    data_temp = pd.read_csv(path, sep='\t', encoding='CP1252', usecols=columns, low_memory=False)
#    if 'NIS_RAD' in data_temp.columns:
#        data_temp.drop_duplicates('NIS_RAD', keep='last', inplace=True)
#        data = data_temp.copy()
#    else:
#        data = data_temp.copy()
#    return data
#
## Funcion para cargar Clientes.txt especificando las columnas
#def cliente(columns=None):
#    path = r'\\gnfbaqcgep1\Planificacion\Desarrollos\BD\14_Clientes\Clientes.txt'
#    data_temp = pd.read_csv(path, sep='\t', encoding='latin1', usecols=columns, low_memory=False)
#    if 'NIS_RAD' in data_temp.columns:
#        data_temp.drop_duplicates('NIS_RAD', keep='last', inplace=True)
#        data = data_temp.copy()
#    else:
#        data = data_temp.copy()
#    return data