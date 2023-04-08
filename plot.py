# coding:utf-8

import sys

import matplotlib.pyplot as plt

if __name__ == '__main__':
    for i in range(1, len(sys.argv)):
        plt.plot([float(s) for s in sys.argv[i].split(",")])
    plt.show()
