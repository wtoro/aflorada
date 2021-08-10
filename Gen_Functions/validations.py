
def val_max_column_value(data, path_data, name_column, value):
    path_data = str(path_data)
    name_column = str(name_column)
    value = int(value)
    if int(max(data[name_column])) != value:
        text = "La BD %s se encuentra desactualizada: En la columna <%s> el maximo no es %d."%(path_data,name_column,
                                                                                               value)
        print(text)
        exit()
    return