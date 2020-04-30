import matplotlib.pyplot as plt
import csv

x = []
y = []

with open('logs/balances.log','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    for row in plots:
        x.append(float(row[20]))
        y.append(float(row[22]))

plt.step(range(len(x)), y, label='accumulated')
plt.step(range(len(x)), x, label='per operation')
plt.ylabel('USD')
plt.xlabel('op number')
plt.title('Accumulated and per Opertation profit')
plt.legend()
plt.grid()
plt.show()