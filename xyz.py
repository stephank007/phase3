import pandas as pd
import numpy as np
import numpy.ma as ma

def is_0():
    print('is_0')
    return 0

def is_1():
    print('is_1')
    return 0

def is_2():
    print('is_2')
    return 0

def is_3():
    print('is_3')
    return 0

def is_4():
    print('is_4')
    return 0

def is_5():
    print('is_5')
    return 0

def is_6():
    print('is_6')
    return 0

def is_7():
    print('is_7')
    return 0

functions = [is_0, is_1, is_2, is_3, is_5, is_5, is_6, is_7]

a = None
b = 'abd'
c = 'abc'

a_ind = 0 if a is None else 1
b_ind = 0 if b is None else 1
c_ind = 0 if c in ['abc'] else 1

filters_mask = '{}{}{}'.format(a_ind, b_ind, c_ind)
print(filters_mask)
filter_index = int(filters_mask, 2)
functions[filter_index]()
