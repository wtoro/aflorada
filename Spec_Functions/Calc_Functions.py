# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------
SCRIPT CON LAS FUNCIONES REQUERIDAS PARA EL CALCULO DEL PROMEDIO Y NORMALIZACIONES DE CONSUMOS
-----------------------------------------------------------------------------------------------
Fecha: 19 de Febrero de 2018
Autor: Rubén D. González R.
"""
# ---------------------------------------------------------------------------------------------
# Importando librerías y funciones necesarias
#----------------------------------------------------------------------------------------------
import pandas as pd
import numpy as np
from Gen_Functions.Basics import array_multiplication
# ---------------------------------------------------------------------------------------------
# Función para encontrar y reemplazar consumos negativos
#----------------------------------------------------------------------------------------------
def negatives_replace(Consumos):
    # Validando los consumos negativos
    Neg_Index=np.where(Consumos<0);
    # Obteniendo el tamaño de la tupla con los índices de los negativos
    Num_Neg=len(Neg_Index[0]);
    # Reemplazando los negativos por ceros
    for Ind in range(0,Num_Neg):
        row=Neg_Index[0][Ind];
        col=Neg_Index[1][Ind];
        Consumos[row,col]=0;
    return Consumos
# ---------------------------------------------------------------------------------------------
# Validación de vacíos de consumo (nuevos suministros y vacíos aislados)
#----------------------------------------------------------------------------------------------
def empty_consumptions(Consumos):
    # Validando los consumos vacíos
    Cons_Vac=np.isnan(Consumos);
    # Convirtiendo de arreglo booleano a arreglo binario
    Cons_Vac=Cons_Vac.astype(int);
    # Localizando los vacíos en el arreglo de consumos
    Vac_Ind=np.where(Cons_Vac==1);
    # Filtrando los NIS únicos que tienen vacíos
    NIS_Vac_Ind=np.unique(Vac_Ind[0]);
    # Ciclo para evaluar cada usuario con vacíos
    for Ind in range(0,len(NIS_Vac_Ind)):
        # Extrayendo los 12 períodos de cada usuario a evaluar
        Cons_Temp=Consumos[NIS_Vac_Ind[Ind],:];
        # Validando los vacíos de dicho usuario
        User_Vac=np.isnan(Cons_Temp);
        # Convirtiendo a arreglo binario desde booleano
        User_Vac=User_Vac.astype(int);
        # Ubicando los vacíos dentro del vector de consumos del usuario
        User_Vac_Ind=np.where(User_Vac==1);
        # Ubicando el último vacío
        Last_Vac=User_Vac_Ind[0][-1];
        # Obteniendo la suma de los elementos del arreglo binario hasta el ultimo vacío
        Vac_Sum=np.sum(User_Vac[0:Last_Vac+1]);
        # Validando cual de los 2 casos es (nuevo suministro o vacío aislado)
        if (Vac_Sum!=(Last_Vac+1)):
            # En este caso se asumen como vacíos aislados y se reemplazan por ceros
            for Ind2 in range(0,12):
                if (User_Vac[Ind2]==1):
                    Consumos[NIS_Vac_Ind[Ind],Ind2]=0;
            # Nota: Los nuevos suministros se dejan como NaN los períodos faltantes
    return Consumos
# ---------------------------------------------------------------------------------------------
# Validación de días en cero
#----------------------------------------------------------------------------------------------
def zero_days(Dias):
    # Validando los consumos negativos
    Zero_Index=np.where(Dias==0);
    # Obteniendo el tamaño de la tupla con los índices de los negativos
    Num_Zeros=len(Zero_Index[0]);
    # Reemplazando los negativos por ceros
    for Ind in range(0,Num_Zeros):
        row=Zero_Index[0][Ind];
        col=Zero_Index[1][Ind];
        Dias[row,col]=30;
    return Dias
# ---------------------------------------------------------------------------------------------
# Validación de días vacíos luego del merge con los NIS del universo
#----------------------------------------------------------------------------------------------
def empty_days(Dias):
    # Validando los días vacíos luego del merge con los NIS del universo
    Dias_Vac=np.isnan(Dias);
    # Convirtiendo de arreglo booleano a arreglo binario
    Dias_Vac=Dias_Vac.astype(int);
    # Localizando los vacíos en el arreglo de consumos
    Vac_Ind=np.where(Dias_Vac==1);
    # Ciclo para reemplazar todos los vacíos por 30
    for Ind in range(0,len(Vac_Ind[0])):
        Dias[Vac_Ind[0][Ind],Vac_Ind[1][Ind]]=30;
    return Dias
# ---------------------------------------------------------------------------------------------
# Función para el cálculo del promedio de cada NIS en un período
#----------------------------------------------------------------------------------------------
def user_mean(Consumos,Dias):
    # Multiplicando los consumos x 30
    Product_30=array_multiplication(Consumos,30,True);
    # Obteniendo el inverso de cada elemento del arreglo de días
    Inverse_Dias=1./Dias;
    # Dividiendo los consumos entre sus respectivos días para normalizar
    Consumos_Norm=array_multiplication(Product_30,Inverse_Dias,False);
    # Calculando el promedio de cada usuario ignorando los NaN que existan en cada usuario
    Promedio=np.nanmean(Consumos_Norm,1);
    return Promedio
# ---------------------------------------------------------------------------------------------
# Función para normalizar los consumos a 30 días
#----------------------------------------------------------------------------------------------
def cons_norm(Consumos,Dias):
    # Multiplicando los consumos x 30
    Product_30=array_multiplication(Consumos,30,True);
    # Obteniendo el inverso de cada elemento del arreglo de días
    Inverse_Dias=1./Dias;
    # Dividiendo los consumos entre sus respectivos días para normalizar
    Consumos_Norm=array_multiplication(Product_30,Inverse_Dias,False);
    return Consumos_Norm
# ---------------------------------------------------------------------------------------------
# Función para hacer 0 los promedios vacíos
#----------------------------------------------------------------------------------------------
def empty_prom(promedios):
    # Validando los promedios vacíos 
    Prom_Vac=np.isnan(promedios);
    # Convirtiendo de arreglo booleano a arreglo binario
    Prom_Vac=Prom_Vac.astype(int);
    # Localizando los vacíos en el arreglo de promedios
    Vac_Ind=np.where(Prom_Vac==1);
    # Ciclo para reemplazar todos los vacíos por 0
    for Ind in range(0,len(Vac_Ind[0])):
        promedios[Vac_Ind[0][Ind],Vac_Ind[1][Ind]]=0;
    return promedios
# ---------------------------------------------------------------------------------------------
# Función para hacer 0 la aflorada de consumos vacíos en el período de afloramiento 
#----------------------------------------------------------------------------------------------
def empty_cons_per_aflo(consumos,promedios):
    # Validando los consumos vacíos
    Cons_Vac=np.isnan(consumos);
    # Convirtiendo de arreglo booleano a arreglo binario
    Cons_Vac=Cons_Vac.astype(int);
    # Localizando los vacíos en el arreglo de consumos
    Vac_Ind=np.where(Cons_Vac==1);
    # Ciclo para reemplazar todos los vacíos por 0 para que la aflorada sea 0
    for Ind in range(0,len(Vac_Ind[0])):
        consumos[Vac_Ind[0][Ind],Vac_Ind[1][Ind]]=0;
        promedios[Vac_Ind[0][Ind],Vac_Ind[1][Ind]]=0;
    return (consumos,promedios)