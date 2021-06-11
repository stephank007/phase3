import re

a_string = 'א.	אריק 1 4 קרבabc moshe /~!- (מנהל מחסן) 123 , איתי שמר(קצין ...קישור נמלים)'
b_string = 'אריק 1 4 קרבabc moshe'

# x = re.sub('[^A-Za-z0-9א-ת]*', ' ', my_string)

a1 = [character for character in a_string if character.isalnum()]
a1 = ''.join(a1)

a2 = [character for character in b_string if character.isalnum()]
a2 = ''.join(a1)

print(a1[0:10] in a1)