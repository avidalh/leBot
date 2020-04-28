import matplotlib.pyplot as plt
import csv

x = []
y = []

with open('logs/balances.log','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    for row in plots:
        x.append(float(row[19]))
        y.append(float(row[21]))

plt.step(range(len(x)), y, label='acc Profit')
plt.step(range(len(x)), x, label='Op Profit')
plt.xlabel('x')
plt.ylabel('y')
plt.title('acc Profit')
plt.legend()
plt.grid()
plt.show()