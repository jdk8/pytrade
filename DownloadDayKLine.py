# -*-coding:utf-8-*-

"""
下载日级别K线，不需要修复
"""

import sys

import baostock as bs

from consts.Consts import DAY_KLINE_FILE_FORMAT, CODE_TYPE_ALL, CODE_TYPE_HS300
from domain.Code import Code
from infra.AllCodeRepository import allCodesRepository
from infra.BaoStock import BaoStock
from infra.Hs300CodeRepository import hs300CodesRepository


def getCode2KDayList(code_type):
    last_code = None
    data_list = []
    code2data_list = {}
    for line in open(DAY_KLINE_FILE_FORMAT % code_type):
        segs = line.strip("\n").split(",")
        date = segs[0]
        code = int(segs[1])
        open_price = float(segs[2])
        high_price = float(segs[3])
        low_price = float(segs[4])
        close_price = float(segs[5])
        volume = float(segs[6])
        amount = float(segs[7])
        line = [date, code, open_price, high_price,
                low_price, close_price,
                volume, amount]
        if last_code != code and last_code is not None:
            code2data_list[data_list[0][1]] = data_list
            last_code = code
            data_list = []
            data_list.append(line)
        else:
            last_code = code
            data_list.append(line)
    code2data_list[data_list[0][1]] = data_list
    return code2data_list


def updateByDay(code_type, updateDay):
    # 获取全部代码
    code2DayList = getCode2KDayList(code_type)
    # 写临时文件
    with open((DAY_KLINE_FILE_FORMAT + ".tmp") % code_type, "w") as f:
        for code, DayList in code2DayList.items():
            print(code, (DayList[-1]))
            # 最后一个数据应该是1500点结尾，否则异常
            # 从新浪去下载数据
            # new_datalistday = SinaKLineIntegration.query_history_k_line(
            #     Code.code_id2prefix_code(code), 240, 3)
            if code == -2142 or code == 600068 or code == 600900:
                import copy
                dc = copy.deepcopy(DayList[-1])
                dc[0] = updateDay
                results = [dc]
            else:
                while True:
                    try:
                        results = BaoStock.query_history_k_line_xq(Code.code_id2prefix_dot_code(code),
                                                                   start_date=update_day)
                        break
                    except:
                        pass
            print(code, results)
            if not results:
                print("数据是空的！！", code)
                sys.exit(-1)
            # 看看最后一个是不是已经是今天的数据了
            last_date = DayList[-1][0]
            if last_date == str(updateDay):
                print("今天的数据已经有了", DayList[-1])
                sys.exit(-1)
            oldLen = len(DayList)
            for ele in results:
                newDay = ele[0]
                if newDay != str(updateDay):
                    continue
                openp = float(ele[2])
                high = float(ele[3])
                low = float(ele[4])
                close = float(ele[5])
                volume = float(ele[6])

                DayList.append([newDay, code, openp, high, low, close, volume, float(ele[7])])
            if len(DayList) != oldLen + 1:
                print("今天数据缺失", DayList[-1])
                sys.exit(-1)
            for ele in DayList:
                f.write("%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.6f\n" % (
                    ele[0], ele[1], ele[2], ele[3], ele[4], ele[5], ele[6], ele[7]))


def fetchAll(code_type, download_codes):
    print("下载%s的全部数据，代码数量%d" % (code_type, len(download_codes)))
    # 先进行一个拷贝，避免写错
    with open((DAY_KLINE_FILE_FORMAT + ".tmp") % code_type, "w") as f:
        for code in download_codes:
            print(code.prefix_dot_code)
            data_list = BaoStock.query_history_k_line(code.prefix_dot_code)
            # assert data_list[-1][0] == "2021-04-19"
            for data in data_list:
                day = data[0]
                code = data[1]
                # 获取当天的分时数据
                f.write("%s,%s,%s,%s,%s,%s,%s,%s\n"
                        % (day, code, data[2], data[3],
                           data[4], data[5], data[6], data[7]))

    ##\## 登出系统 ####
    bs.logout()


if __name__ == "__main__":
    code_type = sys.argv[1]  # 股票类型
    if len(sys.argv) == 3:
        update_day = sys.argv[2]
        print("更新%s的数据,类型%s" % (update_day, code_type))
        if code_type == CODE_TYPE_HS300:
            updateByDay(CODE_TYPE_HS300, update_day)
        elif code_type == CODE_TYPE_ALL:
            for industry, codes in allCodesRepository.industry2codes.items():
                updateByDay(industry, update_day)
    else:
        print("全量更新%s" % code_type)
        if code_type == CODE_TYPE_HS300:
            fetchAll(CODE_TYPE_HS300, hs300CodesRepository.codes)
        elif code_type == CODE_TYPE_ALL:
            for industry, codes in allCodesRepository.industry2codes.items():
                fetchAll(industry, codes)
