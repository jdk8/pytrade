# -*-coding:utf-8-*-

import sys

import numpy as np
from sklearn import metrics
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression


class CTRModel(object):

    def __init__(self, model="gbdt"):
        super(CTRModel, self).__init__()
        if model == "gbdt":
            self.clf = GradientBoostingClassifier()
        elif model == "lr":
            self.clf = LogisticRegression()

    def fit(self, X, y):
        self.clf.fit(X, y)

    def predict_proba(self, X):
        return self.clf.predict_proba(X)[:, 1]

    def calc_auc(self, X, y):
        try:
            probs = self.predict_proba(X)
            return metrics.roc_auc_score(y, probs)
        except:
            return -1


class ModelData(object):
    DATE_INDEX = 0
    CODE_INDEX = 1
    PROFIT_INDEX = 2
    PREDICT_INDEX = 3

    def __init__(self, data_file_path, train_date_range, test_date_range=None):
        super(ModelData, self).__init__()
        self.data = np.loadtxt(data_file_path, delimiter=",")
        self.train_date_range = train_date_range
        self.test_date_range = test_date_range

    def get_data(self, date_range):
        data1 = self.data[self.data[:, ModelData.DATE_INDEX] > date_range[0], :]
        data1 = data1[data1[:, ModelData.DATE_INDEX] < date_range[1], :]
        return ModelData.split_X_y(data1)

    def get_train_data(self):
        return self.get_data(self.train_date_range)

    def get_test_data(self):
        return self.get_data(self.test_date_range)

    @staticmethod
    def split_X_y(data):
        return data, data[:, (ModelData.PREDICT_INDEX + 1):], data[:, ModelData.PREDICT_INDEX]


if __name__ == "__main__":
    data_file_file = sys.argv[1]
    train_date_range = [int(s) for s in sys.argv[2].split("-")]
    test_date_range = [int(s) for s in sys.argv[3].split("-")]
    prob_file_path = sys.argv[4]
    mode_data = ModelData(data_file_file, train_date_range, test_date_range)
    _, train_data_X, train_data_y = mode_data.get_train_data()
    print("train_data_X shape=", train_data_X.shape, train_data_y[0:100])
    ctr_model = CTRModel()
    ctr_model.fit(train_data_X, train_data_y)
    test_data, test_data_X, test_data_y = mode_data.get_test_data()
    probs = ctr_model.predict_proba(test_data_X)
    train_auc = ctr_model.calc_auc(train_data_X, train_data_y)
    test_auc = ctr_model.calc_auc(test_data_X, test_data_y)
    print("Args=%s, Train AUC=%f, Test AUC=%f" % (sys.argv, train_auc, test_auc))
    with open(prob_file_path, "w") as f:
        for i in range(test_data.shape[0]):
            date = test_data[i][ModelData.DATE_INDEX]
            code = test_data[i][ModelData.CODE_INDEX]
            profit = test_data[i][ModelData.PROFIT_INDEX]
            prob = probs[i]
            f.write("%d,%d,%f,%f\n" % (date, code, profit, prob))
