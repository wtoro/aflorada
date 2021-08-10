# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------
SCRIPT PARA EL CÁLCULO DE LA ENERGÍA AFLORADA
-----------------------------------------------------------------------------------------------
Fecha: 6 de Marzo de 2018
Consideraciones:
    - E.Aflorada es el delta de consumo de un cliente luego de solucionarle una irregularidad/visitarlo
    - La aflorada se obtiene de restar el consumo luego de la visita con el promedio (12 meses) antes de la visita
    - Para los clientes con multiples visitas (diferentes meses) se trabaja con los promedios de cada visita
    - Clientes con multiples visitas en el mismo período se tomará como válida la que tenga F_UCE más reciente
    - Clientes con consumos negativos o cero se asumiran como cero durante el cálculo
    - Clientes sin consumos previos a la visita se considera todo su consumo aflorado
    - Clientes con menos de 12 períodos antes de la visita se trabaja con los que existan
    - Se incluyen los suministros manuales (escritorio) de OCC del Ing. Carlos M.  
Autor: Rubén D. González R.
"""
# ---------------------------------------------------------------------------------------------
# Importando librerías y/o funciones necesarias
#----------------------------------------------------------------------------------------------
# Importando librerías necesarias
import numpy as np
import pandas as pd
import time
import os, sys
import copy
import sqlalchemy
# Importando las funciones generales y específicas
from Gen_Functions.Load_Data import PCEnergy, write_table, delete_row
from Gen_Functions.Clean_Data import drop_duplicates_by_2columns
from Gen_Functions.Basics import array_multiplication
from Spec_Functions.Calc_Functions import negatives_replace, empty_consumptions, zero_days, empty_days, user_mean, cons_norm, empty_prom, empty_cons_per_aflo
# Importando clases y funciones de manejo de fechas
from datetime import datetime
# ---------------------------------------------------------------------------------------------
# Inicializando variables
#----------------------------------------------------------------------------------------------
# Path
query = """ SELECT * FROM conf_informes."PATHS" WHERE "CLAVE" = 'MATMO_NORMALIZACIONES' """;
extrac = PCEnergy(query);


# Cargando la variable de inicio
#Start_Var=np.load(os.getcwd()+'\\Start_Variable.npy');
Start_Var = np.load(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Start_Variable.npy');
# Iniciando el contador del tiempo
start_time=time.time(); 
# Obteniendo el día actual (el de la ejecucón del script)
Today=datetime.today();
#Today = datetime(2020,1,1); #OJO QUE ESTO NO VAAAAAAAAAAAAAA!!!!!!!!!!!!!!!!!!!!
# Conviertiendo la fecha a String según formato especificado (AAAAMM)
Today=Today.strftime('%Y%m');
# Asignando el período actual a la variable correspondiente (Per)
Per=Today;
# Obteniendo el año y mes del período anterior con respecto a lo fecha actual
Mes_Anterior=str(int(Per[4:6])-1);
Year_Anterior=Per[0:4];
if (Mes_Anterior=='0'):
    Mes_Anterior='12';
    Year_Anterior=str(int(Year_Anterior)-1);
Mes_Anterior=Mes_Anterior.zfill(2);
# Formando el string completo del período anterior al actual
Per_Anterior=Year_Anterior+Mes_Anterior;
# Estableciendo mes y año del período de inicio de las visitas
Mes_Ini='01';
Ano_Ini='2016';
# Estableciendo mes y año del período de afloramiento
Mes_Aflo='02';
Ano_Aflo='2016';
# Numero de períodos desde 201601 hasta el período antes de la primera ejecución del proceso
Tot_Ejec=35;
# ---------------------------------------------------------------------------------------------
# Ejecución del proceso
#----------------------------------------------------------------------------------------------
# Cargando datos de la DB desde la tabla datos básicos
query = """ SELECT "NIS_RAD", "DESC_EST", "TIPO_SUMINISTRO", "MODO_EST", "COD_TAR", "DESC_ASIGNACION" AS "CO_ASIGNACION"
            FROM pcenergy."OP_DATOS_BASICOS"
            WHERE "PERIODO" = '%s'
            """%(Per_Anterior);
datos_basicos = PCEnergy(query);
# Cargando datos de la DB desde la tabla tarifas
query = """ SELECT "COD_TAR", "DESC_TIPO"
            FROM pcenergy."OP_TARIFAS"
            """;
tarifas = PCEnergy(query);
# Cargando datos de la DB desde la tabla localidades
query = """ SELECT "NIS_RAD", "DELEGACION"
            FROM pcenergy."OP_LOCALIDADES"
            """;
localidades = PCEnergy(query);
# Agregando manualmente los suministros del Ing. Carlos Meléndez
#Manual_Users_C=pd.read_excel(os.getcwd()+'\\Sumin_Manuales\\Manual_Users_C.xlsx',sheet_name='Hoja1',names=['NIS_RAD','PER_INT'],usecols=[2,12]);
Manual_Users_C=pd.read_excel(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Sumin_Manuales\\Manual_Users_C.xlsx',sheet_name='Hoja1',names=['NIS_RAD','PER_INT'],usecols=[2,12]);
# Cambiando los tipos de datos de los períodos del DF
Manual_Users_C.PER_INT=Manual_Users_C.PER_INT.astype(str);
# Creando copias de los suministros manuales
Manual_Users_C_Copy=copy.deepcopy(Manual_Users_C);
# Asignando campo de identificación de escritorio para suministros manuales
Manual_Users_C_Copy['ESCRITORIO']='OCC';
# Validando si es primera ejecución o actualización
if (Start_Var[0,0]==0):
    #---------------------------------------------------
    # PARA CUANDO ES LA PRIMERA EJECUCIÓN DEL PROCESO
    #---------------------------------------------------
    # Inicializando los usuarios para la primera ejecución
    NIS_Ant=pd.DataFrame(columns=['NIS_RAD']);
    # Ejecutando el ciclo para construcción de la tabla
    for Num_Ejec in range(0,Tot_Ejec):
        # Obteniendo solo los promedios del período de intervención
        query = """ SELECT "NIS_RAD", "%s"
                    FROM pcenergy."SSI_PROMEDIOS"
                    """%(Ano_Ini+Mes_Ini);
        Prom_Per = PCEnergy(query);
        # Cargando datos de la DB desde la tabla campañas (Ordenes)
        query = """ SELECT "NIS_RAD", "TIPO_OS", "F_UCE", "PERIODO","PLAN_BASE", "PLAN_AGRUPADO", "N1", "N2", "N3", "N4", "N5", "N6", "IRR", "RESULTADO", "NUM_OS"
                    FROM pcenergy."OP_ORDENES" 
                    WHERE ("PERIODO" = '%s') AND ("CAMPAÑA" = 1) AND ("ESTADO_OS" = 'Resuelta')
                    """%(Ano_Ini+Mes_Ini);
        ordenes = PCEnergy(query);
        # Eliminando NIS repetidos del arreglo de ordenes y tomando el que tenga la fecha más reciente
        ordenes = drop_duplicates_by_2columns(ordenes,'NIS_RAD','F_UCE',1);
        # Extrayendo los usuarios del informe a generar
        Users=ordenes['NIS_RAD'].to_frame();
        # Adicionando los suministros manuales de Carlos en los períodos específicos
        if (Ano_Ini+Mes_Ini=='201707') or \
           (Ano_Ini+Mes_Ini=='201708') or \
           (Ano_Ini+Mes_Ini=='201709') or \
           (Ano_Ini+Mes_Ini=='201710') or \
           (Ano_Ini+Mes_Ini=='201711') or \
           (Ano_Ini+Mes_Ini=='201712') or \
           (Ano_Ini+Mes_Ini=='201801'):
               # Extrayendo del DataFrame solo los suministros del período en cuestión
               Man_Per_Users_C=Manual_Users_C[Manual_Users_C['PER_INT'].str.match(Ano_Ini+Mes_Ini)];
               # Dejando solo los NIS_RAD
               Man_Per_Users_C=Man_Per_Users_C[['NIS_RAD']];
               # Agregando los fechas UCE de los usuarios manuales del Ing. Carlos M.
               Copy_C=copy.deepcopy(Man_Per_Users_C);
               Copy_C['PERIODO']=Ano_Ini+Mes_Ini;
               # Uniendolos con los NIS_RAD cargados de ordenes en el período en cuestión
               Users=pd.merge(Users, Man_Per_Users_C, on='NIS_RAD', how='outer');
        # Bloque para seleccionar la última visita
        if (Num_Ejec!=0):
            # Buscando los suministros con multiples visitas
            Usuarios_Rep=pd.merge(Usuarios_Num, Users, on='NIS_RAD', how='inner');
            # Obteniendo el tamaño del dataframe de repetidos
            Rep_Tam=Usuarios_Rep.shape;
            # Actualizando ocurrencias de los repetidos en los DF principales (Eliminando de las copias los campos de los repetidos (visitados nuevamente))
            for index in range(0,Rep_Tam[0]):
                NIS_Rep=Usuarios_Rep['NIS_RAD'][index];
                NIS_Ant = NIS_Ant[NIS_Ant['NIS_RAD'] != NIS_Rep];
                Copia_Prom = Copia_Prom[Copia_Prom['NIS_RAD'] != NIS_Rep];
                Copia_FUCE = Copia_FUCE[Copia_FUCE['NIS_RAD'] != NIS_Rep];             
                Copia_PER=Copia_PER[Copia_PER['NIS_RAD'] != NIS_Rep];
                Copia_PBAS=Copia_PBAS[Copia_PBAS['NIS_RAD'] != NIS_Rep];
                Copia_PAGR=Copia_PAGR[Copia_PAGR['NIS_RAD'] != NIS_Rep];
                Copia_N1=Copia_N1[Copia_N1['NIS_RAD'] != NIS_Rep];
                Copia_N2=Copia_N2[Copia_N2['NIS_RAD'] != NIS_Rep];
                Copia_N3=Copia_N3[Copia_N3['NIS_RAD'] != NIS_Rep];
                Copia_N4=Copia_N4[Copia_N4['NIS_RAD'] != NIS_Rep];
                Copia_N5=Copia_N5[Copia_N5['NIS_RAD'] != NIS_Rep];
                Copia_N6=Copia_N6[Copia_N6['NIS_RAD'] != NIS_Rep];
                Copia_IRR=Copia_IRR[Copia_IRR['NIS_RAD'] != NIS_Rep];
                Copia_RES=Copia_RES[Copia_RES['NIS_RAD'] != NIS_Rep];
                Copia_TIPOS=Copia_TIPOS[Copia_TIPOS['NIS_RAD'] != NIS_Rep];
                Copia_DESCEST=Copia_DESCEST[Copia_DESCEST['NIS_RAD'] != NIS_Rep];
                Copia_TIPSUM=Copia_TIPSUM[Copia_TIPSUM['NIS_RAD'] != NIS_Rep];
                Copia_MOEST=Copia_MOEST[Copia_MOEST['NIS_RAD'] != NIS_Rep];
                Copia_CODTAR=Copia_CODTAR[Copia_CODTAR['NIS_RAD'] != NIS_Rep];
                Copia_COASIG=Copia_COASIG[Copia_COASIG['NIS_RAD'] != NIS_Rep];
                Copia_DEL=Copia_DEL[Copia_DEL['NIS_RAD'] != NIS_Rep];
                Copia_NORM=Copia_NORM[Copia_NORM['NIS_RAD'] != NIS_Rep];
                Copia_NUMOS=Copia_NUMOS[Copia_NUMOS['NIS_RAD'] != NIS_Rep];
        # Agregando los usuarios del período anterior de intervención
        Usuarios_Num=pd.merge(NIS_Ant, Users, on='NIS_RAD', how='outer');
        # Extrayendo el período de intervención
        Users_FUCE = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','F_UCE']], on='NIS_RAD', how='left');
        # Cruzando los usuarios y sus respectivos promedios del mes en cuestión
        Aflo_Prom = pd.merge(Usuarios_Num, Prom_Per, on='NIS_RAD', how='left');
        # Extrayendp el resto de campos que harán parte de la tabla de Aflorada
        Aflo_PER = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','PERIODO']], on='NIS_RAD', how='left');
        Aflo_PBAS = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','PLAN_BASE']], on='NIS_RAD', how='left');
        Aflo_PAGR = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','PLAN_AGRUPADO']], on='NIS_RAD', how='left');
        Aflo_N1 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N1']], on='NIS_RAD', how='left');
        Aflo_N2 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N2']], on='NIS_RAD', how='left');
        Aflo_N3 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N3']], on='NIS_RAD', how='left');
        Aflo_N4 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N4']], on='NIS_RAD', how='left');
        Aflo_N5 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N5']], on='NIS_RAD', how='left');
        Aflo_N6 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N6']], on='NIS_RAD', how='left');
        Aflo_IRR = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','IRR']], on='NIS_RAD', how='left');
        Aflo_RES = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','RESULTADO']], on='NIS_RAD', how='left');
        Aflo_TIPOS = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','TIPO_OS']], on='NIS_RAD', how='left');
        Aflo_NUMOS = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','NUM_OS']], on='NIS_RAD', how='left');
        Aflo_DESCEST = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','DESC_EST']], on='NIS_RAD', how='left');
        Aflo_TIPSUM = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','TIPO_SUMINISTRO']], on='NIS_RAD', how='left');
        Aflo_MOEST = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','MODO_EST']], on='NIS_RAD', how='left');
        Aflo_CODTAR = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','COD_TAR']], on='NIS_RAD', how='left');
        Aflo_COASIG = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','CO_ASIGNACION']], on='NIS_RAD', how='left');
        Aflo_DEL = pd.merge(Usuarios_Num, localidades[['NIS_RAD','DELEGACION']], on='NIS_RAD', how='left');
        # Cargando información de normalizados del período de intervención desde Excel
#        path = r"\\10.240.142.97\02_Dashboards\02_Dashboard_Seguimiento\Matmo_Normalizaciones\Normalizacion\Normalizacion_%s.txt"%(Year_Int+Mes_Int)
        path = extrac['RUTA'][0] + r'\Normalizacion\Normalizacion_%s.txt'%(Year_Int+Mes_Int);
        Norm = pd.read_csv(path, encoding='CP1252', sep='\t', decimal=',', usecols=['NIS_RAD','PERIODO'], low_memory=False)
        Norm = Norm[Norm['PERIODO'] == int(Year_Int+Mes_Int)].reset_index(drop=True)
        Norm['NORMALIZACION'] = 'SI'
        Norm = Norm[['NIS_RAD','NORMALIZACION']]        
        # Creando DataFrame para el campo "Normalizado"
        Aflo_NORM=copy.deepcopy(Usuarios_Num);
        Aflo_NORM['NORMALIZACION']='NO';
        # Ubicando los posiciones de los que se debe reemplazar el NO por SI
        pos = pd.merge(Aflo_NORM[['NIS_RAD']], Norm[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
        # Haciendo el cruce para reemplazar
        Aflo_NORM.loc[pos,'NORMALIZACION'] = pd.merge(Aflo_NORM[['NIS_RAD']], Norm, on='NIS_RAD', how='left').loc[pos,'NORMALIZACION'];
        # Validando la primera ejecución del resto
        if (Num_Ejec!=0):
            # Ubicando los posiciones de los promedios y fec_visita que deben permanecer fijos
            pos = pd.merge(Aflo_Prom[['NIS_RAD']], Copia_Prom[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
            # Obteniendo el mes anterior al de la intervención
            Mes_Ant_Ini=int(Mes_Ini)-1;
            Year_Ant_Ini=Ano_Ini;
            if (Mes_Ant_Ini==0):
                Mes_Ant_Ini=12;
                Year_Ant_Ini=str(int(Ano_Ini)-1);
            Mes_Ant_Ini=str(Mes_Ant_Ini);
            Mes_Ant_Ini=Mes_Ant_Ini.zfill(2);
            # Reemplaznado los promedios y fec_visita actuales por el promedio y fec_visita fijo correspondiente
            Aflo_Prom.loc[pos,'%s'%(Ano_Ini+Mes_Ini)] = pd.merge(Aflo_Prom[['NIS_RAD']], Copia_Prom, on='NIS_RAD', how='left').loc[pos,'%s'%(Year_Ant_Ini+Mes_Ant_Ini)];
            Users_FUCE.loc[pos,'F_UCE'] = pd.merge(Users_FUCE[['NIS_RAD']], Copia_FUCE[['NIS_RAD','F_UCE']], on='NIS_RAD', how='left').loc[pos,'F_UCE'];
            Aflo_PER.loc[pos,'PERIODO'] = pd.merge(Aflo_PER[['NIS_RAD']], Copia_PER[['NIS_RAD','PERIODO']], on='NIS_RAD', how='left').loc[pos,'PERIODO'];
            Aflo_PBAS.loc[pos,'PLAN_BASE'] = pd.merge(Aflo_PBAS[['NIS_RAD']], Copia_PBAS[['NIS_RAD','PLAN_BASE']], on='NIS_RAD', how='left').loc[pos,'PLAN_BASE'];
            Aflo_PAGR.loc[pos,'PLAN_AGRUPADO'] = pd.merge(Aflo_PAGR[['NIS_RAD']], Copia_PAGR[['NIS_RAD','PLAN_AGRUPADO']], on='NIS_RAD', how='left').loc[pos,'PLAN_AGRUPADO'];
            Aflo_N1.loc[pos,'N1'] = pd.merge(Aflo_N1[['NIS_RAD']], Copia_N1[['NIS_RAD','N1']], on='NIS_RAD', how='left').loc[pos,'N1'];
            Aflo_N2.loc[pos,'N2'] = pd.merge(Aflo_N2[['NIS_RAD']], Copia_N2[['NIS_RAD','N2']], on='NIS_RAD', how='left').loc[pos,'N2'];
            Aflo_N3.loc[pos,'N3'] = pd.merge(Aflo_N3[['NIS_RAD']], Copia_N3[['NIS_RAD','N3']], on='NIS_RAD', how='left').loc[pos,'N3'];
            Aflo_N4.loc[pos,'N4'] = pd.merge(Aflo_N4[['NIS_RAD']], Copia_N4[['NIS_RAD','N4']], on='NIS_RAD', how='left').loc[pos,'N4'];
            Aflo_N5.loc[pos,'N5'] = pd.merge(Aflo_N5[['NIS_RAD']], Copia_N5[['NIS_RAD','N5']], on='NIS_RAD', how='left').loc[pos,'N5'];
            Aflo_N6.loc[pos,'N6'] = pd.merge(Aflo_N6[['NIS_RAD']], Copia_N6[['NIS_RAD','N6']], on='NIS_RAD', how='left').loc[pos,'N6'];
            Aflo_IRR.loc[pos,'IRR'] = pd.merge(Aflo_IRR[['NIS_RAD']], Copia_IRR[['NIS_RAD','IRR']], on='NIS_RAD', how='left').loc[pos,'IRR'];
            Aflo_RES.loc[pos,'RESULTADO'] = pd.merge(Aflo_RES[['NIS_RAD']], Copia_RES[['NIS_RAD','RESULTADO']], on='NIS_RAD', how='left').loc[pos,'RESULTADO'];
            Aflo_NUMOS.loc[pos,'NUM_OS'] = pd.merge(Aflo_NUMOS[['NIS_RAD']], Copia_NUMOS[['NIS_RAD','NUM_OS']], on='NIS_RAD', how='left').loc[pos,'NUM_OS'];
            Aflo_TIPOS.loc[pos,'TIPO_OS'] = pd.merge(Aflo_TIPOS[['NIS_RAD']], Copia_TIPOS[['NIS_RAD','TIPO_OS']], on='NIS_RAD', how='left').loc[pos,'TIPO_OS'];
            Aflo_DESCEST.loc[pos,'DESC_EST'] = pd.merge(Aflo_DESCEST[['NIS_RAD']], Copia_DESCEST[['NIS_RAD','DESC_EST']], on='NIS_RAD', how='left').loc[pos,'DESC_EST'];
            Aflo_TIPSUM.loc[pos,'TIPO_SUMINISTRO'] = pd.merge(Aflo_TIPSUM[['NIS_RAD']], Copia_TIPSUM[['NIS_RAD','TIPO_SUMINISTRO']], on='NIS_RAD', how='left').loc[pos,'TIPO_SUMINISTRO'];
            Aflo_MOEST.loc[pos,'MODO_EST'] = pd.merge(Aflo_MOEST[['NIS_RAD']], Copia_MOEST[['NIS_RAD','MODO_EST']], on='NIS_RAD', how='left').loc[pos,'MODO_EST'];
            Aflo_CODTAR.loc[pos,'COD_TAR'] = pd.merge(Aflo_CODTAR[['NIS_RAD']], Copia_CODTAR[['NIS_RAD','COD_TAR']], on='NIS_RAD', how='left').loc[pos,'COD_TAR'];
            Aflo_COASIG.loc[pos,'CO_ASIGNACION'] = pd.merge(Aflo_COASIG[['NIS_RAD']], Copia_COASIG[['NIS_RAD','CO_ASIGNACION']], on='NIS_RAD', how='left').loc[pos,'CO_ASIGNACION'];
            Aflo_DEL.loc[pos,'DELEGACION'] = pd.merge(Aflo_DEL[['NIS_RAD']], Copia_DEL[['NIS_RAD','DELEGACION']], on='NIS_RAD', how='left').loc[pos,'DELEGACION'];
            Aflo_NORM.loc[pos,'NORMALIZACION'] = pd.merge(Aflo_NORM[['NIS_RAD']], Copia_NORM[['NIS_RAD','NORMALIZACION']], on='NIS_RAD', how='left').loc[pos,'NORMALIZACION'];
        # Obteniendo los consumos del mes de evaluación de la aflorada
        query = """ SELECT "NIS_RAD","CSMO_ACTIVA","IND_REAL_ESTIM"
                    FROM pcenergy."OP_CONSUMOS"
                    WHERE "PERIODO" = '%s'
                    """%(Ano_Aflo+Mes_Aflo);
        consumos = PCEnergy(query);
        # Cruzando los usuarios y sus respectivos consumos del mes en cuestión
        Aflo_Cons = pd.merge(Usuarios_Num, consumos, on='NIS_RAD', how='left');
        # Obteniendo los días del mes de evaluación de la aflorada
        query = """ SELECT "NIS_RAD","DIAS", "DIAS_VALIDOS"
                    FROM pcenergy."OP_DIAS"
                    WHERE "PERIODO" = '%s'
                    """%(Ano_Aflo+Mes_Aflo);
        dias = PCEnergy(query);
        # Cruzando los usuarios y sus respectivos días del mes en cuestión
        Aflo_Dias = pd.merge(Usuarios_Num, dias, on='NIS_RAD', how='left');
        # Convirtiendo los DataFrames a sus representaciones NumPy (matrices)
        Aflo_Cons_NP=Aflo_Cons.as_matrix(['CSMO_ACTIVA']);
        Aflo_Dias_NP=Aflo_Dias.as_matrix(['DIAS']);
        Aflo_Prom_NP=Aflo_Prom.as_matrix(['%s'%(Ano_Ini+Mes_Ini)]);
        # Reemplazando los consumos negativos por cero
        Aflo_Cons_NP=negatives_replace(Aflo_Cons_NP);
        # Reemplazando los vacíos aislados por ceros
        Aflo_Cons_NP=empty_consumptions(Aflo_Cons_NP);
        # Reemplazando los días en cero por 30
        Aflo_Dias_NP=zero_days(Aflo_Dias_NP);
        # Reemplazando los días vacíos por 30
        Aflo_Dias_NP=empty_days(Aflo_Dias_NP);
        # Normalizando los consumos del período de la aflorada
        Aflo_Cons_Norm=cons_norm(Aflo_Cons_NP,Aflo_Dias_NP);
        # Validando los promedios vacíos
        Aflo_Prom_NP=empty_prom(Aflo_Prom_NP);
        # Validando que la aflorada sea 0 cuando el consumo es vacío en el per. de afloramiento
        (Aflo_Cons_Norm,Aflo_Prom_NP)=empty_cons_per_aflo(Aflo_Cons_Norm,Aflo_Prom_NP);
        # Calculando la aflorada del período en cuestión
        Aflo_Per=Aflo_Cons_Norm-Aflo_Prom_NP;
        # Redondeando la aflorada para eliminar la coma (,) decimal
        Aflo_Per_Round=np.around(Aflo_Per);
        # Cruzando los Cod_Tar con sus respectivas descripciones
        Aflo_DescTar = pd.merge(Aflo_CODTAR[['COD_TAR']], tarifas, on='COD_TAR', how='left');
        # Creando el DataFrame que irá a PCEnergy
        if (Num_Ejec==0):
            # Para cuando es la primera ejecución
            Aflorada=copy.deepcopy(Usuarios_Num);
            Aflorada['F_UCE']=Users_FUCE[['F_UCE']];
            Aflorada['PER_AFLO']='%s'%(Ano_Aflo+Mes_Aflo);
            Aflorada['PROMEDIO']=Aflo_Prom_NP;
            Aflorada['CONSUMO']=Aflo_Cons_NP;
            Aflorada['AFLORADA']=Aflo_Per_Round;
            Aflorada['PERIODO']=Aflo_PER[['PERIODO']];
            Aflorada['PLAN_BASE']=Aflo_PBAS[['PLAN_BASE']];
            Aflorada['PLAN_AGRUPADO']=Aflo_PAGR[['PLAN_AGRUPADO']];
            Aflorada['N1']=Aflo_N1[['N1']];
            Aflorada['N2']=Aflo_N2[['N2']];
            Aflorada['N3']=Aflo_N3[['N3']];
            Aflorada['N4']=Aflo_N4[['N4']];
            Aflorada['N5']=Aflo_N5[['N5']];
            Aflorada['N6']=Aflo_N6[['N6']];
            Aflorada['IRR']=Aflo_IRR[['IRR']];
            Aflorada['RESULTADO']=Aflo_RES[['RESULTADO']];
            Aflorada['NUM_OS']=Aflo_NUMOS[['NUM_OS']];
            Aflorada['TIPO_OS']=Aflo_TIPOS[['TIPO_OS']];
            Aflorada['DESC_EST']=Aflo_DESCEST[['DESC_EST']];
            Aflorada['TIPO_SUMINISTRO']=Aflo_TIPSUM[['TIPO_SUMINISTRO']];
            Aflorada['MODO_EST']=Aflo_MOEST[['MODO_EST']];
            Aflorada['COD_TAR']=Aflo_CODTAR[['COD_TAR']];
            Aflorada['DESC_TIPO']=Aflo_DescTar[['DESC_TIPO']];
            Aflorada['CO_ASIGNACION']=Aflo_COASIG[['CO_ASIGNACION']];
            Aflorada['DELEGACION']=Aflo_DEL[['DELEGACION']];
            Aflorada['NORMALIZACION']=Aflo_NORM[['NORMALIZACION']];
            Aflorada['IND_REAL_ESTIM']=Aflo_Cons[['IND_REAL_ESTIM']];
            Aflorada['DIAS_VALIDOS']=Aflo_Dias[['DIAS_VALIDOS']];
            if (Ano_Ini+Mes_Ini=='201707') or \
               (Ano_Ini+Mes_Ini=='201708') or \
               (Ano_Ini+Mes_Ini=='201709') or \
               (Ano_Ini+Mes_Ini=='201710') or \
               (Ano_Ini+Mes_Ini=='201711') or \
               (Ano_Ini+Mes_Ini=='201712') or \
               (Ano_Ini+Mes_Ini=='201801'):
                   # Agregandole las fechas a los suministros del Ing. Carlos M.
                   pos = pd.merge(Aflorada[['NIS_RAD']], Copy_C[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
                   Aflorada.loc[pos,'PERIODO'] = pd.merge(Aflorada[['NIS_RAD']], Copy_C, on='NIS_RAD', how='left').loc[pos,'PERIODO'];
            # Reseteando los index de los DF
            Aflorada.reset_index(drop=True,inplace=True);
            # Eliminando NIS repetidos y dejando su ultimo periodo para la copia del cache
            PERIODO_temp = drop_duplicates_by_2columns(Aflorada,'NIS_RAD','PERIODO',1)[['NIS_RAD','PERIODO']];
            Aflo_PER=PERIODO_temp[['NIS_RAD','PERIODO']];
        else:
            # Para los períodos que siguen
            Aflo_Temp=copy.deepcopy(Usuarios_Num);
            Aflo_Temp['F_UCE']=Users_FUCE[['F_UCE']];
            Aflo_Temp['PER_AFLO']='%s'%(Ano_Aflo+Mes_Aflo);
            Aflo_Temp['PROMEDIO']=Aflo_Prom_NP;
            Aflo_Temp['CONSUMO']=Aflo_Cons_NP;
            Aflo_Temp['AFLORADA']=Aflo_Per_Round;
            Aflo_Temp['PERIODO']=Aflo_PER[['PERIODO']];
            Aflo_Temp['PLAN_BASE']=Aflo_PBAS[['PLAN_BASE']];
            Aflo_Temp['PLAN_AGRUPADO']=Aflo_PAGR[['PLAN_AGRUPADO']];
            Aflo_Temp['N1']=Aflo_N1[['N1']];
            Aflo_Temp['N2']=Aflo_N2[['N2']];
            Aflo_Temp['N3']=Aflo_N3[['N3']];
            Aflo_Temp['N4']=Aflo_N4[['N4']];
            Aflo_Temp['N5']=Aflo_N5[['N5']];
            Aflo_Temp['N6']=Aflo_N6[['N6']];
            Aflo_Temp['IRR']=Aflo_IRR[['IRR']];
            Aflo_Temp['RESULTADO']=Aflo_RES[['RESULTADO']];
            Aflo_Temp['NUM_OS']=Aflo_NUMOS[['NUM_OS']];
            Aflo_Temp['TIPO_OS']=Aflo_TIPOS[['TIPO_OS']];
            Aflo_Temp['DESC_EST']=Aflo_DESCEST[['DESC_EST']];
            Aflo_Temp['TIPO_SUMINISTRO']=Aflo_TIPSUM[['TIPO_SUMINISTRO']];
            Aflo_Temp['MODO_EST']=Aflo_MOEST[['MODO_EST']];
            Aflo_Temp['COD_TAR']=Aflo_CODTAR[['COD_TAR']];
            Aflo_Temp['DESC_TIPO']=Aflo_DescTar[['DESC_TIPO']];
            Aflo_Temp['CO_ASIGNACION']=Aflo_COASIG[['CO_ASIGNACION']];
            Aflo_Temp['DELEGACION']=Aflo_DEL[['DELEGACION']];
            Aflo_Temp['NORMALIZACION']=Aflo_NORM[['NORMALIZACION']];
            Aflo_Temp['IND_REAL_ESTIM']=Aflo_Cons[['IND_REAL_ESTIM']];
            Aflo_Temp['DIAS_VALIDOS']=Aflo_Dias[['DIAS_VALIDOS']];
            if (Ano_Ini+Mes_Ini=='201707') or \
               (Ano_Ini+Mes_Ini=='201708') or \
               (Ano_Ini+Mes_Ini=='201709') or \
               (Ano_Ini+Mes_Ini=='201710') or \
               (Ano_Ini+Mes_Ini=='201711') or \
               (Ano_Ini+Mes_Ini=='201712') or \
               (Ano_Ini+Mes_Ini=='201801'):
                   # Reseteando los index de los DF
                   Aflo_Temp.reset_index(drop=True,inplace=True);
                   Copy_C.reset_index(drop=True,inplace=True);
                   # Agregandole las fechas a los suministros del Ing. Carlos M.
                   pos = pd.merge(Aflo_Temp[['NIS_RAD']], Copy_C[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
                   Aflo_Temp.loc[pos,'PERIODO'] = pd.merge(Aflo_Temp[['NIS_RAD']], Copy_C, on='NIS_RAD', how='left').loc[pos,'PERIODO'];
            # Concatenando en el DataFrame principal
            frames=[Aflorada,Aflo_Temp];
            Aflorada=pd.concat(frames);
            # Reseteando los index de los DF
            Aflorada.reset_index(drop=True,inplace=True);
            # Eliminando NIS repetidos y dejando su ultimo periodo para la copia del cache
            PERIODO_temp = drop_duplicates_by_2columns(Aflorada,'NIS_RAD','PERIODO',1)[['NIS_RAD','PERIODO']];
            Aflo_PER=PERIODO_temp[['NIS_RAD','PERIODO']];
            # Borrando el DataFrame temporal
            del Aflo_Temp
        # Guardando el registro de los NIS para el siguiente período
        NIS_Ant=Usuarios_Num;
        # Incrementando el período de intervención y promedios
        Mes_Ini=int(Mes_Ini)+1;
        # Validando que no se salga del ciclo anual
        if (Mes_Ini==13):
            Mes_Ini=1;
            Ano_Ini=str(int(Ano_Ini)+1);
        Mes_Ini=str(Mes_Ini);    
        Mes_Ini=Mes_Ini.zfill(2);
        # Incrementando el período de afloramiento
        Mes_Aflo=int(Mes_Aflo)+1;
        # Validando que no se salga del ciclo anual
        if (Mes_Aflo==13):
            Mes_Aflo=1;
            Ano_Aflo=str(int(Ano_Aflo)+1);
        Mes_Aflo=str(Mes_Aflo)
        Mes_Aflo=Mes_Aflo.zfill(2);
        # Creando copia de los dataframe con los promedios y las fechas
        Copia_Prom=Aflo_Prom;
        Copia_FUCE=Users_FUCE;
        Copia_PER=Aflo_PER;
        Copia_PBAS=Aflo_PBAS;
        Copia_PAGR=Aflo_PAGR;
        Copia_N1=Aflo_N1;
        Copia_N2=Aflo_N2;
        Copia_N3=Aflo_N3;
        Copia_N4=Aflo_N4;
        Copia_N5=Aflo_N5;
        Copia_N6=Aflo_N6;
        Copia_IRR=Aflo_IRR;
        Copia_RES=Aflo_RES;
        Copia_NUMOS=Aflo_NUMOS;
        Copia_TIPOS=Aflo_TIPOS;
        Copia_DESCEST=Aflo_DESCEST;
        Copia_TIPSUM=Aflo_TIPSUM;
        Copia_MOEST=Aflo_MOEST;
        Copia_CODTAR=Aflo_CODTAR;
        Copia_COASIG=Aflo_COASIG;
        Copia_DEL=Aflo_DEL;
        Copia_NORM=Aflo_NORM;
        # Mensaje de progreso del proceso
        print('\nProgreso del proceso: '+'%s'%(str(Num_Ejec+1))+' de 35 Iteraciones');
    # Reseteando los index de los DF
    Aflorada.reset_index(drop=True,inplace=True);
    Manual_Users_C_Copy.reset_index(drop=True,inplace=True);
    # Agregando el campo de identificación de escritorio a la tabla de Aflorada
    Aflorada['ESCRITORIO']='Terreno';
    pos = pd.merge(Aflorada[['NIS_RAD']], Manual_Users_C_Copy[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
    Aflorada.loc[pos,'ESCRITORIO'] = pd.merge(Aflorada[['NIS_RAD']], Manual_Users_C_Copy, on='NIS_RAD', how='left').loc[pos,'ESCRITORIO'];
    # Guardando la tabla de aflorada
#    Aflorada.to_pickle(os.getcwd()+'\\Data\\Aflorada.npy');
    Aflorada.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Aflorada.npy');
    # Guardando los DFs que se requieren para las ejecuciones posteriores
    # Guardando el de NIS anteriores
#    NIS_Ant.to_pickle(os.getcwd()+'\\Data\\NIS_Ant.npy');
    NIS_Ant.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\NIS_Ant.npy');
    # Guardando el de Promedios 
#    Copia_Prom.to_pickle(os.getcwd()+'\\Data\\Copia_Prom.npy');
    Copia_Prom.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_Prom.npy');
    # Guardando el de FUCEs
#    Copia_FUCE.to_pickle(os.getcwd()+'\\Data\\Copia_FUCE.npy');
    Copia_FUCE.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_FUCE.npy');
    # Guardando el resto de campos de la tabla
#    Copia_PER.to_pickle(os.getcwd()+'\\Data\\Copia_PER.npy');
    Copia_PER.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PER.npy');
#    Copia_PBAS.to_pickle(os.getcwd()+'\\Data\\Copia_PBAS.npy');
    Copia_PBAS.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PBAS.npy');
#    Copia_PAGR.to_pickle(os.getcwd()+'\\Data\\Copia_PAGR.npy');
    Copia_PAGR.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PAGR.npy');
#    Copia_N1.to_pickle(os.getcwd()+'\\Data\\Copia_N1.npy');
    Copia_N1.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N1.npy');
#    Copia_N2.to_pickle(os.getcwd()+'\\Data\\Copia_N2.npy');
    Copia_N2.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N2.npy');
#    Copia_N3.to_pickle(os.getcwd()+'\\Data\\Copia_N3.npy');
    Copia_N3.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N3.npy');
#    Copia_N4.to_pickle(os.getcwd()+'\\Data\\Copia_N4.npy');
    Copia_N4.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N4.npy');
#    Copia_N5.to_pickle(os.getcwd()+'\\Data\\Copia_N5.npy');
    Copia_N5.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N5.npy');
#    Copia_N6.to_pickle(os.getcwd()+'\\Data\\Copia_N6.npy');
    Copia_N6.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N6.npy');
#    Copia_IRR.to_pickle(os.getcwd()+'\\Data\\Copia_IRR.npy');
    Copia_IRR.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_IRR.npy');
#    Copia_RES.to_pickle(os.getcwd()+'\\Data\\Copia_RES.npy');
    Copia_RES.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_RES.npy');
#    Copia_NUMOS.to_pickle(os.getcwd()+'\\Data\\Copia_NUMOS.npy');
    Copia_NUMOS.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_NUMOS.npy');
#    Copia_TIPOS.to_pickle(os.getcwd()+'\\Data\\Copia_TIPOS.npy');
    Copia_TIPOS.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_TIPOS.npy');
#    Copia_DESCEST.to_pickle(os.getcwd()+'\\Data\\Copia_DESCEST.npy');
    Copia_DESCEST.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_DESCEST.npy');
#    Copia_TIPSUM.to_pickle(os.getcwd()+'\\Data\\Copia_TIPSUM.npy');
    Copia_TIPSUM.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_TIPSUM.npy');
#    Copia_MOEST.to_pickle(os.getcwd()+'\\Data\\Copia_MOEST.npy');
    Copia_MOEST.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_MOEST.npy');
#    Copia_CODTAR.to_pickle(os.getcwd()+'\\Data\\Copia_CODTAR.npy');
    Copia_CODTAR.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_CODTAR.npy'); 
#    Copia_COASIG.to_pickle(os.getcwd()+'\\Data\\Copia_COASIG.npy');
    Copia_COASIG.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_COASIG.npy');
#    Copia_DEL.to_pickle(os.getcwd()+'\\Data\\Copia_DEL.npy');
    Copia_DEL.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_DEL.npy');
#    Copia_NORM.to_pickle(os.getcwd()+'\\Data\\Copia_NORM.npy');
    Copia_NORM.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_NORM.npy');
    
    # Creando la tabla aflorada en la BD PCEnergy
#    engine = 'postgresql+psycopg2://usrPcEnergy:M4y02019@10.251.139.182:5432/PCEnergy';
#    con = sqlalchemy.create_engine(engine);
#    Aflorada.to_sql('SSI_AFLORADA', con=con, schema='pcenergy', if_exists="append", index=False, chunksize=int(len(Aflorada)/10000));
    write_table(Aflorada, 'SSI_AFLORADA'); # ----------------------------------    
    
    # Actualizando la variable de inicio
    Start_Var[0,0]=Start_Var[0,0]+1;
else:
    #------------------------------------------------
    # PARA CUANDO ES UNA ACTUALIZACION DE LA TABLA
    #------------------------------------------------
    # Cargando los DataFrames que se requieren para la ejecución
#    NIS_Ant=pd.read_pickle(os.getcwd()+'\\Data\\NIS_Ant.npy');
#    Copia_Prom=pd.read_pickle(os.getcwd()+'\\Data\\Copia_Prom.npy');
#    Copia_FUCE=pd.read_pickle(os.getcwd()+'\\Data\\Copia_FUCE.npy');
#    Copia_PER=pd.read_pickle(os.getcwd()+'\\Data\\Copia_PER.npy');
#    Copia_PBAS=pd.read_pickle(os.getcwd()+'\\Data\\Copia_PBAS.npy');
#    Copia_PAGR=pd.read_pickle(os.getcwd()+'\\Data\\Copia_PAGR.npy');
#    Copia_N1=pd.read_pickle(os.getcwd()+'\\Data\\Copia_N1.npy');
#    Copia_N2=pd.read_pickle(os.getcwd()+'\\Data\\Copia_N2.npy');
#    Copia_N3=pd.read_pickle(os.getcwd()+'\\Data\\Copia_N3.npy');
#    Copia_N4=pd.read_pickle(os.getcwd()+'\\Data\\Copia_N4.npy');
#    Copia_N5=pd.read_pickle(os.getcwd()+'\\Data\\Copia_N5.npy');
#    Copia_N6=pd.read_pickle(os.getcwd()+'\\Data\\Copia_N6.npy');
#    Copia_IRR=pd.read_pickle(os.getcwd()+'\\Data\\Copia_IRR.npy');
#    Copia_RES=pd.read_pickle(os.getcwd()+'\\Data\\Copia_RES.npy');
#    Copia_NUMOS=pd.read_pickle(os.getcwd()+'\\Data\\Copia_NUMOS.npy');
#    Copia_TIPOS=pd.read_pickle(os.getcwd()+'\\Data\\Copia_TIPOS.npy');
#    Copia_DESCEST=pd.read_pickle(os.getcwd()+'\\Data\\Copia_DESCEST.npy');
#    Copia_TIPSUM=pd.read_pickle(os.getcwd()+'\\Data\\Copia_TIPSUM.npy');
#    Copia_MOEST=pd.read_pickle(os.getcwd()+'\\Data\\Copia_MOEST.npy');
#    Copia_CODTAR=pd.read_pickle(os.getcwd()+'\\Data\\Copia_CODTAR.npy');
#    Copia_COASIG=pd.read_pickle(os.getcwd()+'\\Data\\Copia_COASIG.npy');
#    Copia_DEL=pd.read_pickle(os.getcwd()+'\\Data\\Copia_DEL.npy');
#    Copia_NORM=pd.read_pickle(os.getcwd()+'\\Data\\Copia_NORM.npy');
    # -------------------
    NIS_Ant=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\NIS_Ant.npy');
    if "DESC_EST" in NIS_Ant:
        del NIS_Ant['DESC_EST']

#Esto se agrega para filtrar los nis de Caribe MAr, solo aplica para cuando se toma de PCEnergy ECA        
#    NIS_ANT_Original = NIS_Ant.copy()
#    datos = datos_basicos[['NIS_RAD','DESC_EST']]
#    nis_cs = pd.merge(NIS_Ant,datos, on ='NIS_RAD', how='left')
#    
#    cond=~nis_cs['DESC_EST'].isnull()
#    NIS_Ant= nis_cs.loc[cond]
#    
#    del datos, nis_cs
 
    Copia_Prom=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_Prom.npy');
    Copia_FUCE=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_FUCE.npy');
    Copia_PER=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PER.npy');
    Copia_PBAS=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PBAS.npy');
    Copia_PAGR=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PAGR.npy');
    Copia_N1=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N1.npy');
    Copia_N2=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N2.npy');
    Copia_N3=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N3.npy');
    Copia_N4=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N4.npy');
    Copia_N5=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N5.npy');
    Copia_N6=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N6.npy');
    Copia_IRR=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_IRR.npy');
    Copia_RES=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_RES.npy');
    Copia_NUMOS=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_NUMOS.npy');
    Copia_TIPOS=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_TIPOS.npy');
    Copia_DESCEST=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_DESCEST.npy');
    Copia_TIPSUM=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_TIPSUM.npy');
    Copia_MOEST=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_MOEST.npy');
    Copia_CODTAR=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_CODTAR.npy');
    Copia_COASIG=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_COASIG.npy');
    Copia_DEL=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_DEL.npy');
    Copia_NORM=pd.read_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_NORM.npy');
    # -------------------
    # Obteniendo el año y mes del período de afloramiento con respecto a lo fecha actual
    Mes_Ant=str(int(Per[4:6])-1);
    Year_Ant=Per[0:4];
    if (Mes_Ant=='0'):
        Mes_Ant='12';
        Year_Ant=str(int(Year_Ant)-1);
    Mes_Ant=Mes_Ant.zfill(2);
    # Obteniendo el año y mes del período de intervención con respecto a lo fecha actual
    Mes_Int=str(int(Per[4:6])-2);
    Year_Int=Per[0:4];
    if (Mes_Int=='0'):
        Mes_Int='12';
        Year_Int=str(int(Year_Int)-1);
    elif (Mes_Int=='-1'):
        Mes_Int='11';
        Year_Int=str(int(Year_Int)-1);    
    Mes_Int=Mes_Int.zfill(2);
    # Obteniendo solo los promedios del período de intervención
    query = """ SELECT "NIS_RAD", "%s"
                FROM pcenergy."SSI_PROMEDIOS" 
                """%(Year_Int+Mes_Int);
    Prom_Per = PCEnergy(query);
    # Cargando datos de la DB desde la tabla campañas (Ordenes)
    query = """ SELECT "NIS_RAD", "TIPO_OS", "F_UCE", "PERIODO","PLAN_BASE", "PLAN_AGRUPADO", "N1", "N2", "N3", "N4", "N5", "N6", "IRR", "RESULTADO", "NUM_OS"
                FROM pcenergy."OP_ORDENES" 
                WHERE ("PERIODO" = '%s') AND ("CAMPAÑA" = 1) AND ("ESTADO_OS" = 'Resuelta')
                """%(Year_Int+Mes_Int);
    ordenes = PCEnergy(query);
    # Eliminando NIS repetidos del arreglo de ordenes y tomando el que tenga la fecha más reciente
    ordenes = drop_duplicates_by_2columns(ordenes,'NIS_RAD','F_UCE',1);
    # Extrayendo los usuarios del informe a generar
    Users=ordenes['NIS_RAD'].to_frame();
    # Adicionando los suministros manuales de Carlos en los períodos específicos
    if (Year_Int+Mes_Int=='201801'):
       # Extrayendo del DataFrame solo los suministros del período en cuestión
       Man_Per_Users_C=Manual_Users_C[Manual_Users_C['PER_INT'].str.match(Year_Int+Mes_Int)];
       # Dejando solo los NIS_RAD
       Man_Per_Users_C=Man_Per_Users_C[['NIS_RAD']];
       # Agregando los fechas UCE de los usuarios manuales del Ing. Carlos M.
       Copy_C=copy.deepcopy(Man_Per_Users_C);
       Copy_C['PERIODO']=Year_Int+Mes_Int;
       # Uniendolos con los NIS_RAD cargados de ordenes en el período en cuestión
       Users=pd.merge(Users, Man_Per_Users_C, on='NIS_RAD', how='outer');
    # Buscando los suministros con multiples visitas
    Usuarios_Rep=pd.merge(NIS_Ant, Users, on='NIS_RAD', how='inner');
    # Obteniendo el tamaño del dataframe de repetidos
    Rep_Tam=Usuarios_Rep.shape;
    # Actualizando ocurrencias de los repetidos en los DF principales
    for index in range(0,Rep_Tam[0]):
        NIS_Rep=Usuarios_Rep['NIS_RAD'][index];
        NIS_Ant = NIS_Ant[NIS_Ant['NIS_RAD'] != NIS_Rep];
        Copia_Prom = Copia_Prom[Copia_Prom['NIS_RAD'] != NIS_Rep];
        Copia_FUCE = Copia_FUCE[Copia_FUCE['NIS_RAD'] != NIS_Rep];
        Copia_PER=Copia_PER[Copia_PER['NIS_RAD'] != NIS_Rep];
        Copia_PBAS=Copia_PBAS[Copia_PBAS['NIS_RAD'] != NIS_Rep];
        Copia_PAGR=Copia_PAGR[Copia_PAGR['NIS_RAD'] != NIS_Rep];
        Copia_N1=Copia_N1[Copia_N1['NIS_RAD'] != NIS_Rep];
        Copia_N2=Copia_N2[Copia_N2['NIS_RAD'] != NIS_Rep];
        Copia_N3=Copia_N3[Copia_N3['NIS_RAD'] != NIS_Rep];
        Copia_N4=Copia_N4[Copia_N4['NIS_RAD'] != NIS_Rep];
        Copia_N5=Copia_N5[Copia_N5['NIS_RAD'] != NIS_Rep];
        Copia_N6=Copia_N6[Copia_N6['NIS_RAD'] != NIS_Rep];
        Copia_IRR=Copia_IRR[Copia_IRR['NIS_RAD'] != NIS_Rep];
        Copia_RES=Copia_RES[Copia_RES['NIS_RAD'] != NIS_Rep];
        Copia_TIPOS=Copia_TIPOS[Copia_TIPOS['NIS_RAD'] != NIS_Rep];
        Copia_DESCEST=Copia_DESCEST[Copia_DESCEST['NIS_RAD'] != NIS_Rep];
        Copia_TIPSUM=Copia_TIPSUM[Copia_TIPSUM['NIS_RAD'] != NIS_Rep];
        Copia_MOEST=Copia_MOEST[Copia_MOEST['NIS_RAD'] != NIS_Rep];
        Copia_CODTAR=Copia_CODTAR[Copia_CODTAR['NIS_RAD'] != NIS_Rep];
        Copia_COASIG=Copia_COASIG[Copia_COASIG['NIS_RAD'] != NIS_Rep];
        Copia_DEL=Copia_DEL[Copia_DEL['NIS_RAD'] != NIS_Rep];
        Copia_NORM=Copia_NORM[Copia_NORM['NIS_RAD'] != NIS_Rep];
        Copia_NUMOS=Copia_NUMOS[Copia_NUMOS['NIS_RAD'] != NIS_Rep];
    # Agregando los usuarios del período anterior de intervención
    Usuarios_Num=pd.merge(NIS_Ant, Users, on='NIS_RAD', how='outer');
    # Extrayendo el período de intervención
    Users_FUCE = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','F_UCE']], on='NIS_RAD', how='left');
    # Cruzando los usuarios y sus respectivos promedios del mes en cuestión
    Aflo_Prom = pd.merge(Usuarios_Num, Prom_Per, on='NIS_RAD', how='left');
    # Extrayendp el resto de campos que harán parte de la tabla de Aflorada
    Aflo_PER = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','PERIODO']], on='NIS_RAD', how='left');
    Aflo_PBAS = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','PLAN_BASE']], on='NIS_RAD', how='left');
    Aflo_PAGR = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','PLAN_AGRUPADO']], on='NIS_RAD', how='left');
    Aflo_N1 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N1']], on='NIS_RAD', how='left');
    Aflo_N2 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N2']], on='NIS_RAD', how='left');
    Aflo_N3 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N3']], on='NIS_RAD', how='left');
    Aflo_N4 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N4']], on='NIS_RAD', how='left');
    Aflo_N5 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N5']], on='NIS_RAD', how='left');
    Aflo_N6 = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','N6']], on='NIS_RAD', how='left');
    Aflo_IRR = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','IRR']], on='NIS_RAD', how='left');
    Aflo_RES = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','RESULTADO']], on='NIS_RAD', how='left');
    Aflo_TIPOS = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','TIPO_OS']], on='NIS_RAD', how='left');
    Aflo_NUMOS = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','NUM_OS']], on='NIS_RAD', how='left');
    Aflo_DESCEST = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','DESC_EST']], on='NIS_RAD', how='left');
    Aflo_TIPSUM = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','TIPO_SUMINISTRO']], on='NIS_RAD', how='left');
    Aflo_MOEST = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','MODO_EST']], on='NIS_RAD', how='left');
    Aflo_CODTAR = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','COD_TAR']], on='NIS_RAD', how='left');
    Aflo_COASIG = pd.merge(Usuarios_Num, datos_basicos[['NIS_RAD','CO_ASIGNACION']], on='NIS_RAD', how='left');
    Aflo_DEL = pd.merge(Usuarios_Num, localidades[['NIS_RAD','DELEGACION']], on='NIS_RAD', how='left');
    # Cargando información de normalizados del período de intervención desde Excel
#    path = r"\\10.240.142.97\02_Dashboards\02_Dashboard_Seguimiento\Matmo_Normalizaciones\Normalizacion\Normalizacion_%s.txt"%(Year_Int+Mes_Int)
    path = extrac['RUTA'][0] + r'\Normalizacion\Normalizacion_%s.txt'%(Year_Int+Mes_Int);
    Norm = pd.read_csv(path, encoding='CP1252', sep='\t', decimal=',', usecols=['NIS_RAD','PERIODO'], low_memory=False)
    Norm = Norm[Norm['PERIODO'] == int(Year_Int+Mes_Int)].reset_index(drop=True)
    ################# Aquí tocaría eliminar duplicados por orden(y categoriía) para que solo quede una orden(que se considera normalizada), pa curarnos en salud. ######################################
    Norm['NORMALIZACION'] = 'SI'
    Norm = Norm[['NIS_RAD','NORMALIZACION']]
    # Creando DataFrame para el campo "Normalizado"
    Aflo_NORM=copy.deepcopy(Usuarios_Num);
    Aflo_NORM['NORMALIZACION']='NO';
    # Ubicando los posiciones de los que se debe reemplazar el NO por SI
    pos = pd.merge(Aflo_NORM[['NIS_RAD']], Norm[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
    # Haciendo el cruce para reemplazar
    Aflo_NORM.loc[pos,'NORMALIZACION'] = pd.merge(Aflo_NORM[['NIS_RAD']], Norm, on='NIS_RAD', how='left').loc[pos,'NORMALIZACION'];
    # Ubicando los posiciones de los promedios y fec_visita que deben permanecer fijos
    pos = pd.merge(Aflo_Prom[['NIS_RAD']], Copia_Prom[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
    # Obteniendo el mes anterior al de la intervención
    Mes_Ant_Ini=int(Mes_Int)-1;
    Year_Ant_Ini=Year_Int;
    if (Mes_Ant_Ini==0):
        Mes_Ant_Ini=12;
        Year_Ant_Ini=str(int(Year_Int)-1);
    Mes_Ant_Ini=str(Mes_Ant_Ini);
    Mes_Ant_Ini=Mes_Ant_Ini.zfill(2);
    # Reemplaznado los promedios y fec_visita actuales por el promedio y fec_visita fijo correspondiente
    Aflo_Prom.loc[pos,'%s'%(Year_Int+Mes_Int)] = pd.merge(Aflo_Prom[['NIS_RAD']], Copia_Prom, on='NIS_RAD', how='left').loc[pos,'%s'%(Year_Ant_Ini+Mes_Ant_Ini)];
    Users_FUCE.loc[pos,'F_UCE'] = pd.merge(Users_FUCE[['NIS_RAD']], Copia_FUCE[['NIS_RAD','F_UCE']], on='NIS_RAD', how='left').loc[pos,'F_UCE'];
    Aflo_PER.loc[pos,'PERIODO'] = pd.merge(Aflo_PER[['NIS_RAD']], Copia_PER[['NIS_RAD','PERIODO']], on='NIS_RAD', how='left').loc[pos,'PERIODO'];
    Aflo_PBAS.loc[pos,'PLAN_BASE'] = pd.merge(Aflo_PBAS[['NIS_RAD']], Copia_PBAS[['NIS_RAD','PLAN_BASE']], on='NIS_RAD', how='left').loc[pos,'PLAN_BASE'];
    Aflo_PAGR.loc[pos,'PLAN_AGRUPADO'] = pd.merge(Aflo_PAGR[['NIS_RAD']], Copia_PAGR[['NIS_RAD','PLAN_AGRUPADO']], on='NIS_RAD', how='left').loc[pos,'PLAN_AGRUPADO'];
    Aflo_N1.loc[pos,'N1'] = pd.merge(Aflo_N1[['NIS_RAD']], Copia_N1[['NIS_RAD','N1']], on='NIS_RAD', how='left').loc[pos,'N1'];
    Aflo_N2.loc[pos,'N2'] = pd.merge(Aflo_N2[['NIS_RAD']], Copia_N2[['NIS_RAD','N2']], on='NIS_RAD', how='left').loc[pos,'N2'];
    Aflo_N3.loc[pos,'N3'] = pd.merge(Aflo_N3[['NIS_RAD']], Copia_N3[['NIS_RAD','N3']], on='NIS_RAD', how='left').loc[pos,'N3'];
    Aflo_N4.loc[pos,'N4'] = pd.merge(Aflo_N4[['NIS_RAD']], Copia_N4[['NIS_RAD','N4']], on='NIS_RAD', how='left').loc[pos,'N4'];
    Aflo_N5.loc[pos,'N5'] = pd.merge(Aflo_N5[['NIS_RAD']], Copia_N5[['NIS_RAD','N5']], on='NIS_RAD', how='left').loc[pos,'N5'];
    Aflo_N6.loc[pos,'N6'] = pd.merge(Aflo_N6[['NIS_RAD']], Copia_N6[['NIS_RAD','N6']], on='NIS_RAD', how='left').loc[pos,'N6'];
    Aflo_IRR.loc[pos,'IRR'] = pd.merge(Aflo_IRR[['NIS_RAD']], Copia_IRR[['NIS_RAD','IRR']], on='NIS_RAD', how='left').loc[pos,'IRR'];
    Aflo_NUMOS.loc[pos,'NUM_OS'] = pd.merge(Aflo_NUMOS[['NIS_RAD']], Copia_NUMOS[['NIS_RAD','NUM_OS']], on='NIS_RAD', how='left').loc[pos,'NUM_OS'];
    Aflo_RES.loc[pos,'RESULTADO'] = pd.merge(Aflo_RES[['NIS_RAD']], Copia_RES[['NIS_RAD','RESULTADO']], on='NIS_RAD', how='left').loc[pos,'RESULTADO'];
    Aflo_TIPOS.loc[pos,'TIPO_OS'] = pd.merge(Aflo_TIPOS[['NIS_RAD']], Copia_TIPOS[['NIS_RAD','TIPO_OS']], on='NIS_RAD', how='left').loc[pos,'TIPO_OS'];
    Aflo_DESCEST.loc[pos,'DESC_EST'] = pd.merge(Aflo_DESCEST[['NIS_RAD']], Copia_DESCEST[['NIS_RAD','DESC_EST']], on='NIS_RAD', how='left').loc[pos,'DESC_EST'];
    Aflo_TIPSUM.loc[pos,'TIPO_SUMINISTRO'] = pd.merge(Aflo_TIPSUM[['NIS_RAD']], Copia_TIPSUM[['NIS_RAD','TIPO_SUMINISTRO']], on='NIS_RAD', how='left').loc[pos,'TIPO_SUMINISTRO'];
    Aflo_MOEST.loc[pos,'MODO_EST'] = pd.merge(Aflo_MOEST[['NIS_RAD']], Copia_MOEST[['NIS_RAD','MODO_EST']], on='NIS_RAD', how='left').loc[pos,'MODO_EST'];
    Aflo_CODTAR.loc[pos,'COD_TAR'] = pd.merge(Aflo_CODTAR[['NIS_RAD']], Copia_CODTAR[['NIS_RAD','COD_TAR']], on='NIS_RAD', how='left').loc[pos,'COD_TAR'];
    Aflo_COASIG.loc[pos,'CO_ASIGNACION'] = pd.merge(Aflo_COASIG[['NIS_RAD']], Copia_COASIG[['NIS_RAD','CO_ASIGNACION']], on='NIS_RAD', how='left').loc[pos,'CO_ASIGNACION'];      
    Aflo_DEL.loc[pos,'DELEGACION'] = pd.merge(Aflo_DEL[['NIS_RAD']], Copia_DEL[['NIS_RAD','DELEGACION']], on='NIS_RAD', how='left').loc[pos,'DELEGACION'];
    Aflo_NORM.loc[pos,'NORMALIZACION'] = pd.merge(Aflo_NORM[['NIS_RAD']], Copia_NORM[['NIS_RAD','NORMALIZACION']], on='NIS_RAD', how='left').loc[pos,'NORMALIZACION'];
    # Obteniendo los consumos del mes de evaluación de la aflorada
    query = """ SELECT "NIS_RAD","CSMO_ACTIVA","IND_REAL_ESTIM"
                FROM pcenergy."OP_CONSUMOS"
                WHERE "PERIODO" = '%s'
                """%(Year_Ant+Mes_Ant);
    consumos = PCEnergy(query);
    # Cruzando los usuarios y sus respectivos consumos del mes en cuestión
    Aflo_Cons = pd.merge(Usuarios_Num, consumos, on='NIS_RAD', how='left');
    # Obteniendo los días del mes de evaluación de la aflorada
    query = """ SELECT "NIS_RAD","DIAS", "DIAS_VALIDOS"
                FROM pcenergy."OP_DIAS"
                WHERE "PERIODO" = '%s'
                """%(Year_Ant+Mes_Ant);
    dias = PCEnergy(query);
    # Cruzando los usuarios y sus respectivos días del mes en cuestión
    Aflo_Dias = pd.merge(Usuarios_Num, dias, on='NIS_RAD', how='left');
    Aflo_Dias=Aflo_Dias.drop_duplicates(subset=['NIS_RAD'])
    # Convirtiendo los DataFrames a sus representaciones NumPy (matrices)
    Aflo_Cons_NP=Aflo_Cons.as_matrix(['CSMO_ACTIVA']);
    Aflo_Dias_NP=Aflo_Dias.as_matrix(['DIAS']);
    Aflo_Prom_NP=Aflo_Prom.as_matrix(['%s'%(Year_Int+Mes_Int)]);
    # Reemplazando los consumos negativos por cero
    Aflo_Cons_NP=negatives_replace(Aflo_Cons_NP);
    # Reemplazando los vacíos aislados por ceros
    Aflo_Cons_NP=empty_consumptions(Aflo_Cons_NP);
    # Reemplazando los días en cero por 30
    Aflo_Dias_NP=zero_days(Aflo_Dias_NP);
    # Reemplazando los días vacíos por 30
    Aflo_Dias_NP=empty_days(Aflo_Dias_NP);
    # Normalizando los consumos del período de la aflorada
    Aflo_Cons_Norm=cons_norm(Aflo_Cons_NP,Aflo_Dias_NP);
    # Validando los promedios vacíos
    Aflo_Prom_NP=empty_prom(Aflo_Prom_NP);
    # Validando que la aflorada sea 0 cuando el consumo es vacío en el per. de afloramiento
    (Aflo_Cons_Norm,Aflo_Prom_NP)=empty_cons_per_aflo(Aflo_Cons_Norm,Aflo_Prom_NP);
    # Calculando la aflorada del período en cuestión
    Aflo_Per=Aflo_Cons_Norm-Aflo_Prom_NP;
    # Redondeando la aflorada para eliminar la coma (,) decimal
    Aflo_Per_Round=np.around(Aflo_Per);
    # Cruzando los Cod_Tar con sus respectivas descripciones
    Aflo_DescTar = pd.merge(Aflo_CODTAR[['COD_TAR']], tarifas, on='COD_TAR', how='left');
    # Concatenando al DataFrame de Aflorada
    Aflo_Temp=copy.deepcopy(Usuarios_Num);
    Aflo_Temp['F_UCE']=Users_FUCE[['F_UCE']];
    Aflo_Temp['PER_AFLO']='%s'%(Year_Ant+Mes_Ant);
    Aflo_Temp['PROMEDIO']=Aflo_Prom_NP;
    Aflo_Temp['CONSUMO']=Aflo_Cons_NP;
    Aflo_Temp['AFLORADA']=Aflo_Per_Round;
    Aflo_Temp['PERIODO']=Aflo_PER[['PERIODO']];
    Aflo_Temp['PLAN_BASE']=Aflo_PBAS[['PLAN_BASE']];
    Aflo_Temp['PLAN_AGRUPADO']=Aflo_PAGR[['PLAN_AGRUPADO']];
    Aflo_Temp['N1']=Aflo_N1[['N1']];
    Aflo_Temp['N2']=Aflo_N2[['N2']];
    Aflo_Temp['N3']=Aflo_N3[['N3']];
    Aflo_Temp['N4']=Aflo_N4[['N4']];
    Aflo_Temp['N5']=Aflo_N5[['N5']];
    Aflo_Temp['N6']=Aflo_N6[['N6']];
    Aflo_Temp['IRR']=Aflo_IRR[['IRR']];
    Aflo_Temp['RESULTADO']=Aflo_RES[['RESULTADO']];
    Aflo_Temp['NUM_OS']=Aflo_NUMOS[['NUM_OS']];
    Aflo_Temp['TIPO_OS']=Aflo_TIPOS[['TIPO_OS']];
    Aflo_Temp['DESC_EST']=Aflo_DESCEST[['DESC_EST']];
    Aflo_Temp['TIPO_SUMINISTRO']=Aflo_TIPSUM[['TIPO_SUMINISTRO']];
    Aflo_Temp['MODO_EST']=Aflo_MOEST[['MODO_EST']];
    Aflo_Temp['COD_TAR']=Aflo_CODTAR[['COD_TAR']];
    Aflo_Temp['DESC_TIPO']=Aflo_DescTar[['DESC_TIPO']];
    Aflo_Temp['CO_ASIGNACION']=Aflo_COASIG[['CO_ASIGNACION']];
    Aflo_Temp['DELEGACION']=Aflo_DEL[['DELEGACION']];
    Aflo_Temp['NORMALIZACION']=Aflo_NORM[['NORMALIZACION']];
    Aflo_Temp['IND_REAL_ESTIM']=Aflo_Cons[['IND_REAL_ESTIM']];
    Aflo_Temp['DIAS_VALIDOS']=Aflo_Dias[['DIAS_VALIDOS']];
    if (Year_Int+Mes_Int=='201801'):
        # Reseteando los index de los DF
        Aflo_Temp.reset_index(drop=True,inplace=True);
        Copy_C.reset_index(drop=True,inplace=True);
        # Agregandole las fechas a los suministros del Ing. Carlos M.
        pos = pd.merge(Aflo_Temp[['NIS_RAD']], Copy_C[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
        Aflo_Temp.loc[pos,'PERIODO'] = pd.merge(Aflo_Temp[['NIS_RAD']], Copy_C, on='NIS_RAD', how='left').loc[pos,'PERIODO'];
    # Reseteando los index de los DF
    Aflo_Temp.reset_index(drop=True,inplace=True);
    Manual_Users_C_Copy.reset_index(drop=True,inplace=True);
    # Agregando el campo de identificación de escritorio a la tabla de Aflorada
    Aflo_Temp['ESCRITORIO']='Terreno';
    pos = pd.merge(Aflo_Temp[['NIS_RAD']], Manual_Users_C_Copy[['NIS_RAD']], on='NIS_RAD', how='left', indicator=True)['_merge'] == 'both';
    Aflo_Temp.loc[pos,'ESCRITORIO'] = pd.merge(Aflo_Temp[['NIS_RAD']], Manual_Users_C_Copy, on='NIS_RAD', how='left').loc[pos,'ESCRITORIO'];
    # -------------------------------------------------------------------------###
    # Creando campo NORMALIZACION_2
    Aflo_Temp['NORMALIZACION_2'] = 'NO';
    # Periodos de los archivos Normalizacion
    # Al culminar un año, su ultimo periodo (diciembre) debe agregarse al listado Per
    Per = ['201612','201712','201812','201912',Year_Int+Mes_Int];
    for i in Per:
        path = extrac['RUTA'][0] + r'\Normalizacion\Normalizacion_%s.txt'%(i);
        Norm = pd.read_csv(path, encoding='CP1252', sep='\t', decimal=',', usecols=['NIS_RAD','NUM_OS','UUCC_AGRUP2','PERIODO'], low_memory=False);
        # Obteniendo suministros normalizados de acuerdo al periodo de intervención y su acción
        cond = ((Norm['UUCC_AGRUP2'] == 'Normalización') | (Norm['UUCC_AGRUP2'] == 'Adecuaciones') | \
               (Norm['UUCC_AGRUP2'] == 'Reubicaciones'));        #Esta ultima condicion es solo si nos traemos un solo periodo.
        Norm_2 = Norm.loc[cond].reset_index(drop=True);
        Norm_2 = Norm_2.sort_values(['NUM_OS','PERIODO']).reset_index(drop=True);
        Norm_2 = Norm_2.drop_duplicates(['NUM_OS','PERIODO'], keep='last').reset_index(drop=True);
        # Cruzando con el universo principal
        Norm_2['PERIODO'] = Norm_2['PERIODO'].astype(np.str);
        pos = pd.merge(Aflo_Temp[['NUM_OS','PERIODO']], Norm_2[['NUM_OS','PERIODO']], how='left', on=['NUM_OS','PERIODO'], indicator=True)['_merge']=='both';
        Aflo_Temp.loc[pos,'NORMALIZACION_2'] = 'SI';
    # -------------------------------------------------------------------------###
    
    # Limpiando tabla antes del cargue
    delete_row(Per_Anterior, 'SSI_AFLORADA') # --------------------------------
    # Concatenando la tabla de aflorada actual con la histórica de PCEnergy
#    engine = 'postgresql+psycopg2://usrPcEnergy:M4y02019@10.251.139.182:5432/PCEnergy';
#    con = sqlalchemy.create_engine(engine);
#    Aflo_Temp.to_sql('SSI_AFLORADA', con=con, schema='pcenergy', if_exists="append", index=False, chunksize=int(len(Aflo_Temp)/10000));
    write_table(Aflo_Temp, 'SSI_AFLORADA'); # ---------------------------------
    
    # -------------------------------------------------------------------------###
    # Eliminando campo NORMALIZACION_2
    del Aflo_Temp['NORMALIZACION_2'];
    # -------------------------------------------------------------------------###

    # Eliminando NIS repetidos y dejando su ultimo periodo para la copia del cache
    PERIODO_temp = drop_duplicates_by_2columns(Aflo_Temp,'NIS_RAD','PERIODO',1)[['NIS_RAD','PERIODO']];
    Aflo_PER=PERIODO_temp[['NIS_RAD','PERIODO']];
    # Borrando el DataFrame temporal
    del Aflo_Temp
    # Guardando el registro de los NIS para el siguiente período
    NIS_Ant=Usuarios_Num;
    # Creando copia de los dataframe con los promedios y las fechas
    Copia_Prom=Aflo_Prom;
    Copia_FUCE=Users_FUCE;
    Copia_PER=Aflo_PER;
    Copia_PBAS=Aflo_PBAS;
    Copia_PAGR=Aflo_PAGR;
    Copia_N1=Aflo_N1;
    Copia_N2=Aflo_N2;
    Copia_N3=Aflo_N3;
    Copia_N4=Aflo_N4;
    Copia_N5=Aflo_N5;
    Copia_N6=Aflo_N6;
    Copia_IRR=Aflo_IRR;
    Copia_RES=Aflo_RES;
    Copia_NUMOS=Aflo_NUMOS;
    Copia_TIPOS=Aflo_TIPOS;
    Copia_DESCEST=Aflo_DESCEST;
    Copia_TIPSUM=Aflo_TIPSUM;
    Copia_MOEST=Aflo_MOEST;
    Copia_CODTAR=Aflo_CODTAR;
    Copia_COASIG=Aflo_COASIG;
    Copia_DEL=Aflo_DEL;
    Copia_NORM=Aflo_NORM;
    # Guardando los DFs que se requieren para las ejecuciones posteriores
#    NIS_Ant.to_pickle(os.getcwd()+'\\Data\\NIS_Ant.npy');
#    Copia_Prom.to_pickle(os.getcwd()+'\\Data\\Copia_Prom.npy');
#    Copia_FUCE.to_pickle(os.getcwd()+'\\Data\\Copia_FUCE.npy');
    # ------------
    NIS_Ant.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\NIS_Ant.npy');
    Copia_Prom.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_Prom.npy');
    Copia_FUCE.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_FUCE.npy');
    # ------------
    # Guardando el resto de campos de la tabla
#    Copia_PER.to_pickle(os.getcwd()+'\\Data\\Copia_PER.npy');
#    Copia_PBAS.to_pickle(os.getcwd()+'\\Data\\Copia_PBAS.npy');
#    Copia_PAGR.to_pickle(os.getcwd()+'\\Data\\Copia_PAGR.npy');
#    Copia_N1.to_pickle(os.getcwd()+'\\Data\\Copia_N1.npy');
#    Copia_N2.to_pickle(os.getcwd()+'\\Data\\Copia_N2.npy');
#    Copia_N3.to_pickle(os.getcwd()+'\\Data\\Copia_N3.npy');
#    Copia_N4.to_pickle(os.getcwd()+'\\Data\\Copia_N4.npy');
#    Copia_N5.to_pickle(os.getcwd()+'\\Data\\Copia_N5.npy');
#    Copia_N6.to_pickle(os.getcwd()+'\\Data\\Copia_N6.npy');
#    Copia_IRR.to_pickle(os.getcwd()+'\\Data\\Copia_IRR.npy');
#    Copia_RES.to_pickle(os.getcwd()+'\\Data\\Copia_RES.npy');
#    Copia_NUMOS.to_pickle(os.getcwd()+'\\Data\\Copia_NUMOS.npy');
#    Copia_TIPOS.to_pickle(os.getcwd()+'\\Data\\Copia_TIPOS.npy');
#    Copia_DESCEST.to_pickle(os.getcwd()+'\\Data\\Copia_DESCEST.npy');
#    Copia_TIPSUM.to_pickle(os.getcwd()+'\\Data\\Copia_TIPSUM.npy');
#    Copia_MOEST.to_pickle(os.getcwd()+'\\Data\\Copia_MOEST.npy');
#    Copia_CODTAR.to_pickle(os.getcwd()+'\\Data\\Copia_CODTAR.npy');
#    Copia_COASIG.to_pickle(os.getcwd()+'\\Data\\Copia_COASIG.npy');
#    Copia_DEL.to_pickle(os.getcwd()+'\\Data\\Copia_DEL.npy');
#    Copia_NORM.to_pickle(os.getcwd()+'\\Data\\Copia_NORM.npy');
    # ---------------
    Copia_PER.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PER.npy');
    Copia_PBAS.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PBAS.npy');
    Copia_PAGR.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_PAGR.npy');
    Copia_N1.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N1.npy');
    Copia_N2.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N2.npy');
    Copia_N3.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N3.npy');
    Copia_N4.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N4.npy');
    Copia_N5.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N5.npy');
    Copia_N6.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_N6.npy');
    Copia_IRR.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_IRR.npy');
    Copia_RES.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_RES.npy');
    Copia_NUMOS.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_NUMOS.npy');
    Copia_TIPOS.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_TIPOS.npy');
    Copia_DESCEST.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_DESCEST.npy');
    Copia_TIPSUM.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_TIPSUM.npy');
    Copia_MOEST.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_MOEST.npy');
    Copia_CODTAR.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_CODTAR.npy');
    Copia_COASIG.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_COASIG.npy');
    Copia_DEL.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_DEL.npy');
    Copia_NORM.to_pickle(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Data\\Copia_NORM.npy');
    # ---------------
    # Actualizando la variable de inicio
    Start_Var[0,0]=Start_Var[0,0]+1;
# Guardando en el PC la variable de inicio
#np.save(os.getcwd()+'\\Start_Variable.npy',Start_Var);
np.save(os.path.abspath(os.path.dirname(sys.argv[0]))+'\\Start_Variable.npy',Start_Var);
# Mensaje de finalización
print('\nEl proceso se ha completado satisfactoriamente!')   
# Finalizando el contador del tiempo
print("\n--- %s Horas ---" % ((time.time() - start_time)/3600))
print('TP12')
print("Proceso OK")