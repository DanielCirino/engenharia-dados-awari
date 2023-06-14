import math


def fibonacci(limite: int):
    a = 0
    b = 1
    sequencia = [1, 2]

    for i in range(limite):
        num = a + b
        a = b
        b = num

        sequencia.append(num)

        # if(num<=limite):
        #     sequencia.append(num)
        # else:
        #     break
    print(sequencia)


def fatorial(num: int):
    if num == 0: return 1
    f = num * fatorial(num - 1)
    print(f'fatorial({num})={f}')
    return f
