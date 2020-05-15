import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import csv
import datetime as dt


trades = []
acc = []
x = []

with open('logs/balances.log','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    for row in plots:
        trades.append(float(row[20]))
        acc.append(float(row[22]))
        x.append(dt.datetime.strptime(str(row[0][0:19]), '%Y-%m-%d %H:%M:%S'))

fig, ax = plt.subplots()

ax.step(x, acc, label='accumulated')
ax.step(x, trades, label='per operation')
# ax.xaxis.set_major_formatter(myFmt)
ax.xaxis_date()

# ax.ylabel('USD')
# ax.xlabel('op number')
# ax.title('Accumulated and per Opertation profit')

ax.legend()
ax.grid()
plt.show()