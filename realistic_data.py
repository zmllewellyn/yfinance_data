import csv
import yfinance as yf
import math

#######################################################################################################################
# Collect list of data
#######################################################################################################################

file = open("cold_storage\Energy.csv", "r")
list_data = list(csv.reader(file))

#######################################################################################################################
# Download data through yfinance
#######################################################################################################################

print("Downloading Data")
num_points = []
for comp in list_data[1:]:
    ticker = yf.Ticker(comp[0])

    #data = ticker.history(period='1y', interval='1h')
    data = ticker.history(start='2019-05-01', end='2021-04-01', interval='1h')
    data_index = data.index.tolist()
    data_list = data.values.tolist()

    value = []

    i = 0
    for row in data_list:
        value.append([str(data_index[i].to_pydatetime()), row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
        i += 1

    csv_file = open('cold_storage\\yfinance_csv\\' + comp[0] + '_yfinance.csv', "w", newline='')
    writer = csv.writer(csv_file)

    row_table = []
    j = 1
    for row in value:
        row_table = [row[0][:10] + '-' + str(j), row[1], row[2], row[3], row[4], row[5], row[6], row[7]]
        writer.writerow(row_table)

        j += 1
        if j == 8:
            j = 1

    # Ensure that every company can be collected
    writer.writerow(['NAN', 1, 1, 1, 1, 1, 1, 1])

    csv_file.close()

    num_points.append((comp[0], i))

#######################################################################################################################
# Collect a list of the dates
#######################################################################################################################

print("Finding Dates")
num_points.sort(key=lambda x: x[1], reverse=True)

file_1_clean = open("cold_storage\\yfinance_csv\\" + num_points[0][0] + "_yfinance.csv", "r")
data_1_clean_csv = csv.reader(file_1_clean)
data_1_clean = list(data_1_clean_csv)

dates = []
for row in data_1_clean:
    dates.append(row[0])

#######################################################################################################################
# Clean the data
#######################################################################################################################

print("Cleaning Data")
company_estimation = []
for comp in list_data[1:]:
    symbol = comp[0]
    yfinance_count = 0
    estimate_count = 0

    file1 = open("cold_storage\\yfinance_csv\\" + symbol + "_yfinance.csv", "r")
    data1 = list(csv.reader(file1))

    if len(data1) > 0:
        cleaned_price_data = []
        prev_data = data1[0]
        for date in dates:
            tr = False
            for stamp in data1:
                if stamp[0] == date:
                    if stamp[1] != 'nan':
                        cleaned_price_data.append(stamp)
                        prev_data = stamp
                        yfinance_count += 1
                        tr = True
                        break
            if tr is False:
                cleaned_price_data.append([date, prev_data[1], prev_data[2], prev_data[3], prev_data[4], prev_data[5]])
                estimate_count += 1

        csv_file = open('cold_storage\\clean_csv\\' + symbol + '_clean.csv', "w", newline='')
        writer = csv.writer(csv_file)

        row_table = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        writer.writerow(row_table)
        for row in cleaned_price_data:
            row_table = [row[0], row[1], row[2], row[3], row[4], row[4], row[5]]
            writer.writerow(row_table)

        company_estimation.append(
            [symbol, yfinance_count / len(cleaned_price_data), estimate_count / len(cleaned_price_data)])

#######################################################################################################################
# Find the log values and log returns and returns of the stocks
#######################################################################################################################

print("Finding Log Values and Log Returns and returns")
for comp in list_data[1:]:
    # Open up each companies data
    file = open("cold_storage\\clean_csv\\" + comp[0] + "_clean.csv", "r")
    file_data = list(csv.reader(file))
    # Make a list of their returns
    log_values = []
    log_returns = [[0, 0, 0, 0, 0]]
    returns = [[0, 0, 0, 0, 0]]
    i = 1
    while i < len(file_data):
        if file_data[i][0] != 'NAN':
            # Calculate the log values of the OHLC data
            log_op = math.log(float(file_data[i][1]))
            log_high = math.log(float(file_data[i][2]))
            log_low = math.log(float(file_data[i][3]))
            log_close = math.log(float(file_data[i][4]))

            # Append the values to the lists
            log_values.append([file_data[i][0], log_op, log_high, log_low, log_close])

            if i > 1:
                # Calculate the log returns of the OHLC data
                log_ret_op = math.log(float(file_data[i][1])) - math.log(float(file_data[i - 1][1]))
                log_ret_high = math.log(float(file_data[i][2])) - math.log(float(file_data[i - 1][2]))
                log_ret_low = math.log(float(file_data[i][3])) - math.log(float(file_data[i - 1][3]))
                log_ret_close = math.log(float(file_data[i][4])) - math.log(float(file_data[i - 1][4]))

                # Calculate the returns of the OHLC data
                ret_op = float(file_data[i][1]) - float(file_data[i - 1][1])
                ret_high = float(file_data[i][2]) - float(file_data[i - 1][2])
                ret_low = float(file_data[i][3]) - float(file_data[i - 1][3])
                ret_close = float(file_data[i][4]) - float(file_data[i - 1][4])

                # Append the values to the lists
                log_returns.append([file_data[i][0], log_ret_op, log_ret_high, log_ret_low, log_ret_close])
                returns.append([file_data[i][0], ret_op, ret_high, ret_low, ret_close])

        # Update the counter
        i += 1

    log_csv_file = open('cold_storage\\log_csv\\' + comp[0] + '_log.csv', "w", newline='')
    log_writer = csv.writer(log_csv_file)

    log_ret_csv_file = open('cold_storage\\log_return_csv\\' + comp[0] + '_log_return.csv', "w", newline='')
    log_ret_writer = csv.writer(log_ret_csv_file)

    ret_csv_file = open('cold_storage\\return_csv\\' + comp[0] + '_return.csv', "w", newline='')
    ret_writer = csv.writer(ret_csv_file)

    row_table = ['Date', 'Open', 'High', 'Low', 'Close']
    log_writer.writerow(row_table)
    log_ret_writer.writerow(row_table)
    ret_writer.writerow((row_table))

    for row in log_values:
        row_table = [row[0], row[1], row[2], row[3], row[4]]
        log_writer.writerow(row_table)

    for row in log_returns:
        row_table = [row[0], row[1], row[2], row[3], row[4]]
        log_ret_writer.writerow(row_table)

    for row in returns:
        row_table = [row[0], row[1], row[2], row[3], row[4]]
        ret_writer.writerow(row_table)

#######################################################################################################################
# Filter out the weak stocks
#######################################################################################################################

print("Filtering Stocks")
work_comp = []
for comp in list_data[1:]:
    for comp_clean in company_estimation:
        if comp[0] == comp_clean[0]:
            if comp_clean[1] >= 0.9:
                work_comp.append(comp[0])

#######################################################################################################################
# Return the usable stocks
#######################################################################################################################

print("The list of companies is... ")
print(work_comp)
print(" ")