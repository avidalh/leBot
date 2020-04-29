import matplotlib.pyplot as plt
import csv

x = []
y = []

with open('logs/balances.log','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    for row in plots:
        x.append(float(row[20]))
        y.append(float(row[22]))

plt.step(range(len(x)), y, label='accumulated profit')
plt.step(range(len(x)), x, label='operations profit')
plt.xlabel('x')
plt.ylabel('y')
plt.title('acc Profit')
plt.legend()
plt.grid()
plt.show()