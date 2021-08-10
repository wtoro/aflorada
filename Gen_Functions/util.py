import pandas as pd
from simplecrypt import encrypt, decrypt
version_llave1='e47298ehqwidn9qw8uwdw983kmdaosdas-*asasdasro91874nyd923er'

def encryp(cadena='',archivo=''):
    #Encripta el contenido de la variable y lo guara en un archivo
    ciphertext = encrypt(version_llave1, cadena)
    f1 = open(archivo,"bw")
    f1.write(ciphertext)
    f1.close()
    return 'Encriptado'
    
def decryp(archivo):
    #Lee un archivo dessencripta el contenido
    f = open(archivo,"br")
    mensaje1 = f.read()
    f.close()
    plaintext2 = decrypt(version_llave1, mensaje1).decode('utf8')
    df = pd.read_json(plaintext2)
    return df