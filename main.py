#Principal
# Importando las funciones generales y específicas
from Gen_Functions.Load_Data import PCEnergy

query = """ SELECT * FROM conf_informes."PATHS" WHERE "CLAVE" = 'MATMO_NORMALIZACIONES' """
extrac = PCEnergy(query)
print(extrac)
