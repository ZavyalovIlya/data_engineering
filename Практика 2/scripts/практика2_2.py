# -*- coding: utf-8 -*-
"""Практика2_2.ipynb

"""

import numpy as np
import os

matrix = np.load('second_task.npy')

x, y, z = [], [], []

for row in range(matrix.shape[0]):
  for col in range(matrix.shape[1]):
    if matrix[row][col] > 542:
      x.append(row)
      y.append(col)
      z.append(matrix[row][col])

np.savez('second_task_output1.npz', x=x, y=y, z=z)
np.savez_compressed('second_task_output2_compr.npz', x=x, y=y, z=z)

orig_size = os.path.getsize("second_task_output1.npz")
compr_size = os.path.getsize("second_task_output2_compr.npz")

print(f'savez file size = {orig_size} bytes')
print(f'compressed savez file size = {compr_size} bytes')
print(f'compressed file is lighter by {orig_size - compr_size} bytes')