# -*-coding:utf-8-*-

import sys

import baostock as bs

bs.login()


class Code(object):

    def __init__(self, line):
        super(Code, self).__init__()
        segs = line.strip("\n").split(",")
        self.code_id = Code.code2id(segs[1])
        self.name = segs[2]
        self.industry = segs[3]
        self.prefix_code = segs[1].replace(".", "")
        self.prefix_dot_code = segs[1]

    @staticmethod
    def date_str2int(date_str):
        return int(date_str.replace("-", ""))

    @staticmethod
    def code2id(code):
        code_id = int(code[3:])
        if code[0:2] == "sz":
            code_id *= -1
        return code_id

    @staticmethod
    def code_id2prefix_code(code_id):
        if code_id > 0:
            return "sh%d" % code_id
        if code_id < 0:
            code_id = -code_id
            if code_id < 10:
                return "sz00000%d" % code_id
            if code_id < 100:
                return "sz0000%d" % code_id
            if code_id < 1000:
                return "sz000%d" % code_id
            if code_id < 10000:
                return "sz00%d" % code_id
            if code_id < 100000:
                return "sz0%d" % code_id
            return "sz%d" % code_id

    @staticmethod
    def code_id2prefix_dot_code(code_id):
        if code_id > 0:
            return "sh.%d" % code_id
        if code_id < 0:
            code_id = -code_id
            if code_id < 10:
                return "sz.00000%d" % code_id
            if code_id < 100:
                return "sz.0000%d" % code_id
            if code_id < 1000:
                return "sz.000%d" % code_id
            if code_id < 10000:
                return "sz.00%d" % code_id
            if code_id < 100000:
                return "sz.0%d" % code_id
            return "sz.%d" % code_id


codes = []

try:
    for line in open("code.csv", encoding='utf-8'):
        codes.append(Code(line))
except:
    print("no code.csv found")


def query_history_k_line(code, start_date='2019-01-01', end_date='2025-06-03', frequency="d"):
    """
    分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
    """
    while True:
        if frequency in ["5", "30"]:
            print("begin")
            rs = bs.query_history_k_data_plus(code,
                                              "date,code,open,high,low,close,volume,amount",
                                              start_date=start_date, end_date=end_date,
                                              frequency=frequency, adjustflag="3")
            print("end")
        elif frequency in ["d"]:
            rs = bs.query_history_k_data_plus(code,
                                              "date,code,open,high,low,close,volume,turn",
                                              start_date=start_date, end_date=end_date,
                                              frequency=frequency, adjustflag="3")
        else:
            raise Exception("bad frequency")

        if rs.error_msg == "用户未登录":
            print("重新登陆")
            bs.login()
        else:
            break

    klines = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        row_data = rs.get_row_data()
        day = row_data[0]
        code = Code.code2id(row_data[1])
        open_price = float(row_data[2])
        high_price = float(row_data[3])
        low_price = float(row_data[4])
        close_price = float(row_data[5])
        volume = float(row_data[6]) if row_data[6] else 0.0
        # volume = round(volume / 100.0)
        klines.append([day, code, open_price, high_price,
                       low_price, close_price, volume])
    return klines


# day must be 2023-04-06 format
def fetch_m5_by_day(day):
    with open("m5.tmp.csv", "w") as f:
        for code in codes:
            data_list = query_history_k_line(code.prefix_dot_code, start_date=day,
                                             end_date=day, frequency="5")
            print("data_list=", code.code_id, len(data_list))
            day2info = {}
            day2datalist = {}
            for data in data_list:
                day = data[0]
                if day not in day2datalist:
                    day2datalist[day] = [data]
                else:
                    day2datalist[day].append(data)
            items = sorted(day2datalist.items(), key=lambda d: Code.date_str2int(d[0]))
            for k, v in items:
                if (len(v) != 48):
                    print(k, v)
                for i in range(len(v)):
                    index2Time = {0: "-09-35", 1: "-09-40", 2: "-09-45", 3: "-09-50", 4: "-09-55",
                                  5: "-10-00", 6: "-10-05", 7: "-10-10", 8: "-10-15", 9: "-10-20", 10: "-10-25",
                                  11: "-10-30", 12: "-10-35", 13: "-10-40", 14: "-10-45", 15: "-10-50", 16: "-10-55",
                                  17: "-11-00", 18: "-11-05", 19: "-11-10", 20: "-11-15", 21: "-11-20", 22: "-11-25",
                                  23: "-11-30", 24: "-13-05", 25: "-13-10", 26: "-13-15", 27: "-13-20", 28: "-13-25",
                                  29: "-13-30", 30: "-13-35", 31: "-13-40", 32: "-13-45", 33: "-13-50", 34: "-13-55",
                                  35: "-14-00", 36: "-14-05", 37: "-14-10", 38: "-14-15", 39: "-14-20", 40: "-14-25",
                                  41: "-14-30", 42: "-14-35", 43: "-14-40", 44: "-14-45", 45: "-14-50", 46: "-14-55",
                                  47: "-15-00",
                                  }
                    s = v[i][0] + index2Time[i]
                    s = s.replace("-", "")
                    if int(s) not in day2info:
                        f.write("%s,%s,%s,%s,%s,%s,%s\n" % (s,
                                                            v[i][1], v[i][2], v[i][3], v[i][4], v[i][5], v[i][6]))
                    else:
                        sina = day2info[int(s)]
                        f.write("%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f\n" % (s,
                                                                      v[i][1], sina[0], sina[1], sina[2], sina[3],
                                                                      sina[4]))
    bs.logout()


def fetch_m5():
    with open("m5.csv", "w") as f:
        for code in codes:
            data_list = query_history_k_line(code.prefix_dot_code, frequency="5")
            print("data_list=", code.code_id, len(data_list))
            day2info = {}
            day2datalist = {}
            for data in data_list:
                day = data[0]
                if day not in day2datalist:
                    day2datalist[day] = [data]
                else:
                    day2datalist[day].append(data)
            items = sorted(day2datalist.items(), key=lambda d: Code.date_str2int(d[0]))
            for k, v in items:
                if (len(v) != 48):
                    print(k, v)
                for i in range(len(v)):
                    index2Time = {0: "-09-35", 1: "-09-40", 2: "-09-45", 3: "-09-50", 4: "-09-55",
                                  5: "-10-00", 6: "-10-05", 7: "-10-10", 8: "-10-15", 9: "-10-20", 10: "-10-25",
                                  11: "-10-30", 12: "-10-35", 13: "-10-40", 14: "-10-45", 15: "-10-50", 16: "-10-55",
                                  17: "-11-00", 18: "-11-05", 19: "-11-10", 20: "-11-15", 21: "-11-20", 22: "-11-25",
                                  23: "-11-30", 24: "-13-05", 25: "-13-10", 26: "-13-15", 27: "-13-20", 28: "-13-25",
                                  29: "-13-30", 30: "-13-35", 31: "-13-40", 32: "-13-45", 33: "-13-50", 34: "-13-55",
                                  35: "-14-00", 36: "-14-05", 37: "-14-10", 38: "-14-15", 39: "-14-20", 40: "-14-25",
                                  41: "-14-30", 42: "-14-35", 43: "-14-40", 44: "-14-45", 45: "-14-50", 46: "-14-55",
                                  47: "-15-00",
                                  }
                    s = v[i][0] + index2Time[i]
                    s = s.replace("-", "")
                    if int(s) not in day2info:
                        f.write("%s,%s,%s,%s,%s,%s,%s\n" % (s,
                                                            v[i][1], v[i][2], v[i][3], v[i][4], v[i][5], v[i][6]))
                    else:
                        sina = day2info[int(s)]
                        f.write("%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f\n" % (s,
                                                                      v[i][1], sina[0], sina[1], sina[2], sina[3],
                                                                      sina[4]))
    bs.logout()


def fetch_m30_by_day(day):
    with open("m30.tmp.csv", "w") as f:
        for code in codes:
            data_list = query_history_k_line(code.prefix_dot_code, start_date=day,
                                             end_date=day, frequency="30")
            print("data_list=", code.code_id, len(data_list))
            day2info = {}
            day2datalist = {}
            for data in data_list:
                day = data[0]
                if day not in day2datalist:
                    day2datalist[day] = [data]
                else:
                    day2datalist[day].append(data)
            items = sorted(day2datalist.items(), key=lambda d: Code.date_str2int(d[0]))
            for k, v in items:
                if (len(v) != 8):
                    print(k, v)
                for i in range(len(v)):
                    index2Time = {0: "-10-00",
                                  1: "-10-30", 2: "-11-00", 3: "-11-30", 4: "-13-30", 5: "-14-00", 6: "-14-30",
                                  7: "-15-00"}
                    s = v[i][0] + index2Time[i]
                    s = s.replace("-", "")
                    if int(s) not in day2info:
                        f.write("%s,%s,%s,%s,%s,%s,%s\n" % (s,
                                                            v[i][1], v[i][2], v[i][3], v[i][4], v[i][5], v[i][6]))
                    else:
                        sina = day2info[int(s)]
                        f.write("%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f\n" % (s,
                                                                      v[i][1], sina[0], sina[1], sina[2], sina[3],
                                                                      sina[4]))

    ##\## 登出系统 ####
    bs.logout()


def fetch_m30():
    with open("m30.csv", "w") as f:
        for code in codes:
            data_list = query_history_k_line(code.prefix_dot_code, frequency="30")
            print("data_list=", code.code_id, len(data_list))
            day2info = {}
            day2datalist = {}
            for data in data_list:
                day = data[0]
                if day not in day2datalist:
                    day2datalist[day] = [data]
                else:
                    day2datalist[day].append(data)
            items = sorted(day2datalist.items(), key=lambda d: Code.date_str2int(d[0]))
            for k, v in items:
                if (len(v) != 8):
                    print(k, v)
                for i in range(len(v)):
                    index2Time = {0: "-10-00",
                                  1: "-10-30", 2: "-11-00", 3: "-11-30", 4: "-13-30", 5: "-14-00", 6: "-14-30",
                                  7: "-15-00"}
                    s = v[i][0] + index2Time[i]
                    s = s.replace("-", "")
                    if int(s) not in day2info:
                        f.write("%s,%s,%s,%s,%s,%s,%s\n" % (s,
                                                            v[i][1], v[i][2], v[i][3], v[i][4], v[i][5], v[i][6]))
                    else:
                        sina = day2info[int(s)]
                        f.write("%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f\n" % (s,
                                                                      v[i][1], sina[0], sina[1], sina[2], sina[3],
                                                                      sina[4]))

    ##\## 登出系统 ####
    bs.logout()


def fetch_day_by_day(day):
    # 先进行一个拷贝，避免写错
    with open("day.tmp.csv", "w") as f:
        for code in codes:
            print(code.prefix_dot_code)
            data_list = query_history_k_line(code.prefix_dot_code, start_date=day,
                                             end_date=day)
            # assert data_list[-1][0] == "2021-04-19"
            for data in data_list:
                day = data[0]
                code = data[1]
                # 获取当天的分时数据
                f.write("%s,%s,%s,%s,%s,%s,%s\n"
                        % (day, code, data[2], data[3],
                           data[4], data[5], data[6]))

    ##\## 登出系统 ####
    bs.logout()


def fetch_day():
    # 先进行一个拷贝，避免写错
    with open("day.csv", "w") as f:
        for code in codes:
            print(code.prefix_dot_code)
            data_list = query_history_k_line(code.prefix_dot_code)
            # assert data_list[-1][0] == "2021-04-19"
            for data in data_list:
                day = data[0]
                code = data[1]
                # 获取当天的分时数据
                f.write("%s,%s,%s,%s,%s,%s,%s\n"
                        % (day, code, data[2], data[3],
                           data[4], data[5], data[6]))

    ##\## 登出系统 ####
    bs.logout()


if __name__ == "__main__":
    period = sys.argv[1]
    if period == "day":
        if len(sys.argv) == 3:
            update_day = sys.argv[2]
            print("更新%s的数据,类型%s" % (period, update_day))
            fetch_day_by_day(update_day)
        else:
            print("全量更新")
            fetch_day()
    elif period == "m30":
        if len(sys.argv) == 3:
            update_day = sys.argv[2]
            print("更新%s的数据,类型%s" % (period, update_day))
            fetch_m30_by_day(update_day)
        else:
            print("全量更新")
            fetch_m30()
    elif period == "m5":
        if len(sys.argv) == 3:
            update_day = sys.argv[2]
            print("更新%s的数据,类型%s" % (period, update_day))
            fetch_m5_by_day(update_day)
        else:
            print("全量更新")
            fetch_m5()
    elif period == "code":
        code_type = sys.argv[2]
        rs = bs.query_stock_industry()
        code2industry = {}
        while (rs.error_code == '0') & rs.next():
            industry = rs.get_row_data()
            line = "%s,%s,%s,%s" % (industry[0], industry[1], industry[2], industry[3])
            code2industry[industry[1]] = line

        if code_type == "hs300":
            rs = bs.query_hs300_stocks()
            code_sets = []
            while (rs.error_code == '0') & rs.next():
                code_sets.append(rs.get_row_data()[1])
        elif code_type == "zz500":
            rs = bs.query_zz500_stocks()
            code_sets = []
            while (rs.error_code == '0') & rs.next():
                code_sets.append(rs.get_row_data()[1])
        elif code_type == 'sz50':
            rs = bs.query_sz50_stocks()
            code_sets = []
            while (rs.error_code == '0') & rs.next():
                code_sets.append(rs.get_row_data()[1])
        else:
            code_sets = code2industry.keys()

        del_codes = []
        for code in code2industry.keys():
            if code not in code_sets:
                del_codes.append(code)
        for code in del_codes:
            del code2industry[code]

        for line in code2industry.values():
            print(line)
