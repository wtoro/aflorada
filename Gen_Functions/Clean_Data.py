# Funcion para eliminar duplicados en base 2 columnas
def drop_duplicates_by_2columns(data, column1, column2, order):
    # Haciendo una copi del arreglo (Dataframe)
    data_temp = data.copy()
    # Validando que los encabezados sean cadenas
    column1 = str(column1)
    column2 = str(column2)
    # Convirtiendo a Booleano
    order = bool(order)
    # Ordenando según las 2 columnas especificadas 
    data_temp.sort_values([column1, column2], ascending=[True, order], inplace=True)
    # Valido que existan duplicados
    if sum(data_temp.duplicated(column1)) >= 1:
        # Elimino duplicados menos el último
        data_temp.drop_duplicates(subset=column1, keep='last', inplace=True)
        # Se asignan los indices con el nuevo orden
        data_temp.reset_index(inplace=True, drop=True)
        # Modificando la salida de la función
        data_result = data_temp.copy()
    else:
        data_result = data_temp.copy()
    return data_result