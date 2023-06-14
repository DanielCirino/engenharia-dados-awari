import math

import aula_04

if __name__ == '__main__':
    exercicio = input(
        'Informe o no. do exercício\n1 ==> Fibonacci \n2 ==> Fatorial \n3 ==> Frutas \n0 ==> Sair \n--------------\n')

    print(f'Selecionada a opção: {exercicio}')

    if exercicio == '0':
        print('Até breve!')
        exit()

    if exercicio == '1':
        aula_04.fibonacci(20)
        exit()

    if exercicio == '2':
        fatorial = aula_04.fatorial(6)
        print(fatorial)
        exit()

    if exercicio == '3':
        frutas = ['bananas', 'maçãs', 'peras', 'uvas', 'laranjas', 'tomates']
        fruta = input('Informe uma fruta (use 99 para sair): \n----------------\n')
        while fruta!='99':
            frutaExiste = fruta in frutas
            if frutaExiste:
                print(f'Fruta {fruta} está disponível!')
            else:
                frutas.append(fruta)
                print(f'Fruta {fruta} não está disponível! Foi incluída na lista.')
                print(frutas)

            fruta = input('Informe uma fruta (use 99 para sair): \n----------------\n')

        exit()

    print(f'Opção [{exercicio}] inválida!')