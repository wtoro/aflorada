import numpy as np
import win32api as win
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Funcion para generar los periodos
def periods(present_period):
    p0 = datetime.strftime(present_period, '%Y%m')
    p1 = datetime.strftime(present_period - relativedelta(months=1), '%Y%m')
    p13 = datetime.strftime(present_period - relativedelta(months=13), '%Y%m')
    return p0, p1, p13

# Funcion para mover una columna de posicion
def move_column(data, name_column, pos_2):
    data_temp = data.copy()
    name_column = str(name_column)
    pos_2 = int(pos_2)
    pos_1 = data_temp.columns.get_loc(name_column)
    column = data_temp[name_column]
    data_temp.insert(pos_2, 'NAME_TEMP', column)
    del data_temp[name_column]
    data_temp.rename(columns={'NAME_TEMP':name_column}, inplace=True)
    if pos_2 == pos_1:
        text = "La posicion en la cual deseas ubicar la columna <%s> es la que presenta actualmente."%name_column
        win.MessageBox(0, text, 'Advertencia', 0x00001000)
    data_result = data_temp
    return data_result

# Funcion para sumar True consecutivos de una lista del ultimo elemento
def suma_consec(lista):
    suma = 0
    n = 0
    for i in reversed(lista):
        if i:
            for i in reversed(lista[0:len(lista)-n]):
                if i:
                    suma = suma + 1
                else:
                    break
            break
        n = n + 1
    return suma

# Funcion para concatenar elementos de columnas consecutivas
def concat_list(lista):
    result = ''
    for i in lista:
        if i is not np.nan:
            result = str(result) + str(i) + ";"
    if isinstance(result, str) is True:
        result = result[:-1]
    if result == '':
        result = np.nan
    return result

# Función para multiplicar elemento a elemento 2 matrices/vectores
def array_multiplication(Arreglo_A,Arreglo_B,Is_Scalar):
    # Si se desea multiplicar por un escalar, este debe ir siempre en la posición del arreglo B
    # A => Matriz, B => Escalar ----> Switch = True
    # A => Matriz, B => Matriz  ----> Switch = False
    if (Is_Scalar==True):
        Arreglo_AB = Arreglo_B*(np.array(Arreglo_A));
    else:
        Arreglo_AB=(np.array(Arreglo_A))*(np.array(Arreglo_B));
    Arreglo_AB.tolist();
    return Arreglo_AB

