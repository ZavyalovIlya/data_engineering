# -*- coding: utf-8 -*-
"""Практика2_1.ipynb

"""

import numpy as np
import json

matrix = np.load('first_task.npy')

matrix

matrix_calc = {
    'sum': 0,
    'avr': 0,
    'sumMD': 0,
    'avrMD': 0,
    'sumSD': 0,
    'avrSD': 0,
    'max': 4,
    'min': 2
}

matrix_calc['sum'] = matrix.sum()

matrix_calc['avr'] = matrix.mean()

matrix_calc['sumMD'] = matrix.diagonal().sum()

matrix_calc['avrMD'] = matrix_calc['sumMD'] / matrix.diagonal().size

matrix_calc['sumSD'] = np.rot90(matrix).diagonal().sum()

matrix_calc['avrSD'] = matrix_calc['sumSD'] / np.rot90(matrix).diagonal().size

matrix_calc['max'] = matrix.max()

matrix_calc['min'] = matrix.min()

matrix_calc = {key: float(value) for key, value in matrix_calc.items()}

with open('first_task_output.json', 'w') as fl:
    json.dump(matrix_calc, fl)

norm_matrix = matrix / matrix_calc['sum']

np.save('first_task_output2.npy', norm_matrix)