with open("C:/Users/anand/Downloads/2018_Yellow_Taxi_Trip_Data.csv", 'r') as long, open('2018_Yellow_Taxi_Trip_Data_1M.csv', 'w') as short:
    for i, row in enumerate(long):
        if i<1000000:
            short.write(row)
