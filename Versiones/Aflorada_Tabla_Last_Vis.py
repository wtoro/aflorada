# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------
SCRIPT PARA EL CÁLCULO DE LA ENERGÍA AFLORADA
-----------------------------------------------------------------------------------------------
Fecha: 6 de Marzo de 2018
Consideraciones:
    - E.Aflorada es el delta de consumo de un cliente luego de solucionarle una irregularidad/visitarlo
    - La aflorada se obtiene de restar el consumo luego de la visita con el promedio (12 meses) antes de la visita
    - Para los clientes con multiples visitas (diferentes meses) se trabaja con la última visita
    - Clientes con multiples visitas en el mismo período se tomará como válida la que tenga F_UCE más reciente
    - Clientes con consumos negativos o cero se asumiran como cero durante el cálculo
    - Clientes sin consumos previos a la visita se considera todo su consumo aflorado
    - Clientes con menos de 12 períodos antes de la visita se trabaja con los que existan
Autor: Rubén D. González R.
"""
# ---------------------------------------------------------------------------------------------
# Importando librerías y/o funciones necesarias
#----------------------------------------------------------------------------------------------
# Importando librerías necesarias
import numpy as np
import pandas as pd
import time
import os
import copy 
# Importando las funciones generales y específicas
from Gen_Functions.Load_Data import create_table, PCEnergy
from Gen_Functions.Clean_Data import drop_duplicates_by_2columns
from Gen_Functions.Basics import array_multiplication
from Spec_Functions.Calc_Functions import negatives_replace, empty_consumptions, zero_days, empty_days, user_mean, cons_norm, empty_prom, empty_cons_per_aflo
# Importando clases y funciones de manejo de fechas
from datetime import datetime
# ---------------------------------------------------------------------------------------------
# Inicializando variables
#----------------------------------------------------------------------------------------------
# Cargando la variable de inicio
Start_Var=np.load(os.getcwd()+'\\Start_Variable.npy');
# Iniciando el contador del tiempo
start_time=time.time(); 
# Obteniendo el día actual (el de la ejecucón del script)
Today=datetime.today();
# Conviertiendo la fecha a String según formato especificado (AAAAMM)
Today=Today.strftime('%Y%m');
# Asignando el período actual a la variable correspondiente (Per)
Per=Today;
# Estableciendo mes y año del período de inicio de las visitas
Mes_Ini='01';
Ano_Ini='2016';
# Estableciendo mes y año del período de afloramiento
Mes_Aflo='02';
Ano_Aflo='2016';
# Numero de períodos desde 201601 hasta el período  antes de la primera ejecución del proceso
Tot_Ejec=25;
# ---------------------------------------------------------------------------------------------
# Ejecución del proceso
#----------------------------------------------------------------------------------------------
# Cargando datos de la DB desde la tabla promedios
query = """ SELECT *
            FROM pcenergy.promedios 
            """;
promedios = PCEnergy(query);
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
        Prom_Per=promedios[['NIS_RAD','%s'%(Ano_Ini+Mes_Ini)]];
        # Cargando datos de la DB desde la tabla campañas (Ordenes)
        query = """ SELECT "NIS_RAD","PERIODO","F_UCE" 
                    FROM pcenergy.ordenes 
                    WHERE ("PERIODO" = '%s') AND ("CAMPANIA" = 1)
                    """%(Ano_Ini+Mes_Ini);
        ordenes = PCEnergy(query);
        # Eliminando NIS repetidos del arreglo de ordenes y tomando el que tenga la fecha más reciente
        ordenes = drop_duplicates_by_2columns(ordenes,'NIS_RAD','F_UCE',1);
        # Extrayendo los usuarios del informe a generar
        Users=ordenes['NIS_RAD'].to_frame();
        # Bloque para seleccionar la última visita
        if (Num_Ejec!=0):
            # Buscando los suministros con multiples visitas
            Usuarios_Rep=pd.merge(Usuarios_Num, Users, on='NIS_RAD', how='inner');
            # Obteniendo el tamaño del dataframe de repetidos
            Rep_Tam=Usuarios_Rep.shape;
            # Eliminando ocurrencias de los repetidos en los DF principales
            for index in range(0,Rep_Tam[0]):
                NIS_Rep=Usuarios_Rep['NIS_RAD'][index];
                Aflorada = Aflorada[Aflorada['NIS_RAD'] != NIS_Rep];
                NIS_Ant = NIS_Ant[NIS_Ant['NIS_RAD'] != NIS_Rep];
                Copia_Prom = Copia_Prom[Copia_Prom['NIS_RAD'] != NIS_Rep];
                Copia_FUCE = Copia_FUCE[Copia_FUCE['NIS_RAD'] != NIS_Rep];
        # Agregando los usuarios del período anterior de intervención
        Usuarios_Num=pd.merge(NIS_Ant, Users, on='NIS_RAD', how='outer');
        # Extrayendo el período de intervención
        Users_FUCE = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','F_UCE']], on='NIS_RAD', how='left');
        # Cruzando los usuarios y sus respectivos promedios del mes en cuestión
        Aflo_Prom = pd.merge(Usuarios_Num, Prom_Per, on='NIS_RAD', how='left');
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
        # Obteniendo los consumos del mes de evaluación de la aflorada
        query = """ SELECT "NIS_RAD","CSMO_ACTIVA"
                    FROM pcenergy.consumos
                    WHERE "PERIODO" = '%s'
                    """%(Ano_Aflo+Mes_Aflo);
        consumos = PCEnergy(query);
        # Cruzando los usuarios y sus respectivos consumos del mes en cuestión
        Aflo_Cons = pd.merge(Usuarios_Num, consumos, on='NIS_RAD', how='left');
        # Obteniendo los días del mes de evaluación de la aflorada
        query = """ SELECT "NIS_RAD","DIAS"
                    FROM pcenergy.dias
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
        # Creando el DataFrame que irá a PCEnergy
        if (Num_Ejec==0):
            # Para cuando es la primera ejecución
            Aflorada=copy.deepcopy(Usuarios_Num);
            Aflorada['F_UCE']=Users_FUCE[['F_UCE']];
            Aflorada['PER_AFLO']='%s'%(Ano_Aflo+Mes_Aflo);
            Aflorada['PROMEDIO']=Aflo_Prom_NP;
            Aflorada['AFLORADA']=Aflo_Per;
        else:
            # Para los períodos que siguen
            Aflo_Temp=copy.deepcopy(Usuarios_Num);
            Aflo_Temp['F_UCE']=Users_FUCE[['F_UCE']];
            Aflo_Temp['PER_AFLO']='%s'%(Ano_Aflo+Mes_Aflo);
            Aflo_Temp['PROMEDIO']=Aflo_Prom_NP;
            Aflo_Temp['AFLORADA']=Aflo_Per;
            # Concatenando en el DataFrame principal
            frames=[Aflorada,Aflo_Temp];
            Aflorada=pd.concat(frames);
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
        # Mensaje de progreso del proceso
        print('\nProgreso del proceso: '+'%s'%(str(Num_Ejec+1))+' de 25 Iteraciones');
    # Guardando los DFs que se requieren para las ejecuciones posteriores
    # Guardando el de NIS anteriores
    NIS_Ant.to_pickle(os.getcwd()+'\\NIS_Ant.npy');
    # Guardando el de Promedios 
    Copia_Prom.to_pickle(os.getcwd()+'\\Copia_Prom.npy');
    # Guardando el de FUCEs
    Copia_FUCE.to_pickle(os.getcwd()+'\\Copia_FUCE.npy');
    # Creando la tabla aflorada en la BD PCEnergy
    create_table(Aflorada,'aflorada',0);
    # Actualizando la variable de inicio
    Start_Var[0,0]=Start_Var[0,0]+1;
else:
    #------------------------------------------------
    # PARA CUANDO ES UNA ACTUALIZACION DE LA TABLA
    #------------------------------------------------
    # Cargando datos de la DB desde la tabla Aflorada
    query = """ SELECT *
                FROM pcenergy.aflorada 
                """;
    Aflorada = PCEnergy(query);
    # Cargando los DataFrames que se requieren para la ejecución
    NIS_Ant=pd.read_pickle(os.getcwd()+'\\NIS_Ant.npy');
    Copia_Prom=pd.read_pickle(os.getcwd()+'\\Copia_Prom.npy');
    Copia_FUCE=pd.read_pickle(os.getcwd()+'\\Copia_FUCE.npy');
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
    Prom_Per=promedios[['NIS_RAD','%s'%(Year_Int+Mes_Int)]];
    # Cargando datos de la DB desde la tabla campañas (Ordenes)
    query = """ SELECT "NIS_RAD","PERIODO","F_UCE" 
                FROM pcenergy.ordenes 
                WHERE ("PERIODO" = '%s') AND ("CAMPANIA" = 1)
                """%(Year_Int+Mes_Int);
    ordenes = PCEnergy(query);
    # Eliminando NIS repetidos del arreglo de ordenes y tomando el que tenga la fecha más reciente
    ordenes = drop_duplicates_by_2columns(ordenes,'NIS_RAD','F_UCE',1);
    # Extrayendo los usuarios del informe a generar
    Users=ordenes['NIS_RAD'].to_frame();
    # Bloque para seleccionar la última visita
    # Buscando los suministros con multiples visitas
    Usuarios_Rep=pd.merge(NIS_Ant, Users, on='NIS_RAD', how='inner');
    # Obteniendo el tamaño del dataframe de repetidos
    Rep_Tam=Usuarios_Rep.shape;
    # Actualizando ocurrencias de los repetidos en los DF principales
    for index in range(0,Rep_Tam[0]):
        NIS_Rep=Usuarios_Rep['NIS_RAD'][index];
        Aflorada = Aflorada[Aflorada['NIS_RAD'] != NIS_Rep];
        NIS_Ant = NIS_Ant[NIS_Ant['NIS_RAD'] != NIS_Rep];
        Copia_Prom = Copia_Prom[Copia_Prom['NIS_RAD'] != NIS_Rep];
        Copia_FUCE = Copia_FUCE[Copia_FUCE['NIS_RAD'] != NIS_Rep];
    # Agregando los usuarios del período anterior de intervención
    Usuarios_Num=pd.merge(NIS_Ant, Users, on='NIS_RAD', how='outer');
    # Extrayendo el período de intervención
    Users_FUCE = pd.merge(Usuarios_Num, ordenes[['NIS_RAD','F_UCE']], on='NIS_RAD', how='left');
    # Cruzando los usuarios y sus respectivos promedios del mes en cuestión
    Aflo_Prom = pd.merge(Usuarios_Num, Prom_Per, on='NIS_RAD', how='left');
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
    # Obteniendo los consumos del mes de evaluación de la aflorada
    query = """ SELECT "NIS_RAD","CSMO_ACTIVA"
                FROM pcenergy.consumos
                WHERE "PERIODO" = '%s'
                """%(Year_Ant+Mes_Ant);
    consumos = PCEnergy(query);
    # Cruzando los usuarios y sus respectivos consumos del mes en cuestión
    Aflo_Cons = pd.merge(Usuarios_Num, consumos, on='NIS_RAD', how='left');
    # Obteniendo los días del mes de evaluación de la aflorada
    query = """ SELECT "NIS_RAD","DIAS"
                FROM pcenergy.dias
                WHERE "PERIODO" = '%s'
                """%(Year_Ant+Mes_Ant);
    dias = PCEnergy(query);
    # Cruzando los usuarios y sus respectivos días del mes en cuestión
    Aflo_Dias = pd.merge(Usuarios_Num, dias, on='NIS_RAD', how='left');
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
    # Concatenando al DataFrame de Aflorada
    Aflo_Temp=copy.deepcopy(Usuarios_Num);
    Aflo_Temp['F_UCE']=Users_FUCE[['F_UCE']];
    Aflo_Temp['PER_AFLO']='%s'%(Year_Ant+Mes_Ant);
    Aflo_Temp['PROMEDIO']=Aflo_Prom_NP;
    Aflo_Temp['AFLORADA']=Aflo_Per;
    # Concatenando en el DataFrame principal
    frames=[Aflorada,Aflo_Temp];
    Aflorada=pd.concat(frames);
    # Borrando el DataFrame temporal
    del Aflo_Temp
    # Guardando el registro de los NIS para el siguiente período
    NIS_Ant=Usuarios_Num;
    # Creando copia de los dataframe con los promedios y las fechas
    Copia_Prom=Aflo_Prom;
    Copia_FUCE=Users_FUCE;
    # Guardando los DFs que se requieren para las ejecuciones posteriores
    NIS_Ant.to_pickle(os.getcwd()+'\\NIS_Ant.npy');
    Copia_Prom.to_pickle(os.getcwd()+'\\Copia_Prom.npy');
    Copia_FUCE.to_pickle(os.getcwd()+'\\Copia_FUCE.npy');
    # Actualizando la tabla de aflorada en la BD PCEnergy
    create_table(Aflorada,'aflorada',1);
    # Actualizando la variable de inicio
    Start_Var[0,0]=Start_Var[0,0]+1;
# Guardando en el PC la variable de inicio
np.save(os.getcwd()+'\\Start_Variable.npy',Start_Var);
# Mensaje de finalización
print('\nEl proceso se ha completado satisfactoriamente!')   
# Finalizando el contador del tiempo
print("\n--- %s Horas ---" % ((time.time() - start_time)/3600))
    
    
    
    
    

    
    