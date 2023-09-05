from random import randint, random
import os.path

def geraDados(qtd):
    if(os.path.exists('RelacionamentoNoticiasPessoas.csv')):
        escreve = open("RelacionamentoNoticiasPessoas.csv",'a')
    else:
        escreve = open("RelacionamentoNoticiasPessoas.csv",'w')

    qtdNoticias = 60000
    # qtdNoticias = 167053
    qtdPessoas = 10000

    matrizGiganteCheck = [0]* qtdNoticias*qtdPessoas

    i = 0
    while (i<qtd):
        idPessoas = randint(1, 10000)
        # idNoticias = randint(1, 167053) 
        idNoticias = randint(1, 60000) 
        if(matrizGiganteCheck[(idPessoas-1)+(qtdPessoas*(idNoticias-1))]==0):
            escreve.write("{:d},".format(idPessoas)+"{:d}\n".format(idNoticias))     # -1 para tirar o \n
            matrizGiganteCheck[(idPessoas-1)+(qtdPessoas*(idNoticias-1))]=1
            i+=1
    escreve.close()
    print("Fim\n")

#reseta os comandos
if(os.path.exists('RelacionamentoNoticiasPessoas.csv')):
    os.remove("RelacionamentoNoticiasPessoas.csv")

geraDados(50000)
