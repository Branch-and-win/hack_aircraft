import pandas as pd
import pandasql as ps

interval = 5
bus_seats_cnt = 80

data_folder_path = '../data/'
raw_data_folder_path = data_folder_path + 'raw_data/'
opt_data_folder_path = data_folder_path + 'opt_data/'

# Считывание данных по МС ВС
AircraftStands_df  = pd.read_csv(raw_data_folder_path + 'Aircraft_Stands_Public.csv')
# Считывание данных по классам ВС
AirCraftClasses_df = pd.read_csv(raw_data_folder_path + 'AirCraftClasses_Public.csv')
# Считывание данных по расписанию A/D ВС
Timetable_df       = pd.read_csv(raw_data_folder_path + 'Timetable_Public.csv')
# Считывание данных по времени обсуживания ВС
HandlingTime_df    = pd.read_csv(raw_data_folder_path + 'Handling_Time_Public.csv')
# Различные косты для модели
HandlingRates_df   = pd.read_csv(raw_data_folder_path + 'Handling_Rates_Public.csv')

# Создание агрегированных стоянок
def create_aggr_stands(AircraftStands_df):
    columns_list = list(AircraftStands_df.columns.values)
    char_columns_list = columns_list.copy()
    char_columns_list.remove('Aircraft_Stand')
    Stands_df = AircraftStands_df.groupby(char_columns_list, dropna = False, as_index = False).count().rename(columns={'Aircraft_Stand':'StandNum'})
    Stands_df['ID'] = Stands_df.index
    Stands_df['Terminal'] = Stands_df['Terminal'].fillna(-1).astype(int)
    return Stands_df
Stands_df = create_aggr_stands(AircraftStands_df)

# Создание транспонированной таблицы со временем движения автобусов до термила для каждой агрегированной стоянки
def create_transpose_bustime(Stands_df):
    return Stands_df[['ID', '1', '2', '3', '4', '5']].melt(id_vars=['ID'], var_name='Terminal', value_name='BusTime')
BusTime_df = create_transpose_bustime(Stands_df)
# Удаление столбцов после транспонирования
Stands_df.drop(columns=['1', '2', '3', '4', '5'], inplace = True)

# Создание классов ВС
def create_ac_classes(AirCraftClasses_df):
    sqlcode = '''
        select A.Aircraft_Class as FlightClass, A.Max_Seats, lag(Max_Seats, 1, 0) over (order by Max_Seats) as Min_Seats
        from AirCraftClasses_df A
        '''
    return ps.sqldf(sqlcode,locals())
ACClass_df = create_ac_classes(AirCraftClasses_df)

# Вычисление количества автобусов, флага широкофюзеляжного и класса ВС
def create_timetable_w_add(Timetable_df, ACClass_df, bus_seats_cnt = 80):
    sqlcode = '''
        select A.*
            , cast ( {double_BusRequired} as int ) + ( {double_BusRequired} > cast ( {double_BusRequired} as int )) as BusRequired
            , B.FlightClass
        from Timetable_df A
        left join ACClass_df B
        on A.flight_AC_PAX_capacity_total > B.Min_Seats and A.flight_AC_PAX_capacity_total <= B.Max_Seats
        '''.format(double_BusRequired = 'A.flight_PAX * 1.0 / {bus_seats_cnt}'.format(bus_seats_cnt = bus_seats_cnt))
    Flights_df = ps.sqldf(sqlcode,locals())
    Flights_df['ID'] = Flights_df.index
    return Flights_df
Flights_df = create_timetable_w_add(Timetable_df, ACClass_df, bus_seats_cnt = bus_seats_cnt)

# Преобразование костов
def create_rates(HandlingRates_df):
    return HandlingRates_df.set_index('Name').T
HandlingRates_df = create_rates(HandlingRates_df)

# Создание множества моментов времени (с детализацией до interval минут)
def create_time(interval):
    times = [i_time for i_time in range(0, int(24 * 60 / interval))]
    return pd.DataFrame(times, columns =['ID'])
Time_df = create_time(interval = interval)

# Преобразование дат в id интервала
def time_to_interval(df, interval, time_col, interval_col, drop = False):
    sqlcode = '''
        select A.*
            , printf("%d", round((strftime('%H', A.{time_col}) * 60 + strftime('%M', A.{time_col})) / {interval})) as {interval_col}
        from df A
        '''.format(time_col = time_col, interval_col = interval_col, interval = interval)
    result_df = ps.sqldf(sqlcode,locals())
    if drop == True:
        result_df.drop(columns = [time_col], inplace = True)
    return result_df
Flights_df = time_to_interval(Flights_df, interval=interval, time_col = 'flight_datetime', interval_col = 'FlightTime', drop = True)

# Переименование столбцов для оптимизации и загрузка в .csv
def df_to_opt(opt_folder, Flights_df, Stands_df, HandlingTime_df, HandlingRates_df, BusTime_df, Time_df):
    Flights_rename_dict = {    
        'ID'                  : 'ID'     
        , 'flight_ID'         : 'FlightID'
        , 'flight_AD'         : 'FlightAD'
        , 'flight_terminal_#' : 'FlightTerminal'
        , 'FlightClass'       : 'FlightClass'
        , 'BusRequired'       : 'BusRequired'
        , 'FlightTime'        : 'FlightTime'}
    Stands_rename_dict = {         
        'ID'                       : 'ID'
        , 'JetBridge_on_Arrival'   : 'JetBridgeArrival'
        , 'JetBridge_on_Departure' : 'JetBridgeDeparture'
        , 'Terminal'               : 'StandTerminal'
        , 'Taxiing_Time'           : 'TaxingTime'
        , 'StandNum'               : 'StandNum'}
    HandlingTime_rename_dict = {         
        'Aircraft_Class'            : 'ID'
        , 'JetBridge_Handling_Time' : 'BridgeHandlingTime'
        , 'Away_Handling_Time'      : 'BusHandlingTime'}
    HandlingRates_rename_dict = {         
        'Bus_Cost_per_Minute'                        : 'BusCost'
        , 'Away_Aircraft_Stand_Cost_per_Minute'      : 'BusStandCost'
        , 'JetBridge_Aircraft_Stand_Cost_per_Minute' : 'BridgeStandCost'
        , 'Aircraft_Taxiing_Cost_per_Minute'         : 'TaxingCost'}
    BusTime_rename_dict = {         
        'ID'         : 'StandID'
        , 'Terminal' : 'TerminalID'
        , 'BusTime'  : 'BusTime'}
    Time_rename_dict = {         
        'ID' : 'ID'}

    def df_to_csv(df, rename_dict, out_csv_name):
        df.rename(columns = rename_dict)[list(rename_dict.values())].to_csv(out_csv_name, index = False)
    df_to_csv(Flights_df      , Flights_rename_dict      , opt_folder + 'Flights.csv')
    df_to_csv(Stands_df       , Stands_rename_dict       , opt_folder + 'Stands.csv')
    df_to_csv(HandlingTime_df , HandlingTime_rename_dict , opt_folder + 'HandlingTime.csv')
    df_to_csv(HandlingRates_df, HandlingRates_rename_dict, opt_folder + 'HandlingRates.csv')
    df_to_csv(BusTime_df      , BusTime_rename_dict      , opt_folder + 'BusTime.csv')
    df_to_csv(Time_df         , Time_rename_dict         , opt_folder + 'Time.csv')
    
df_to_opt(opt_data_folder_path, Flights_df, Stands_df, HandlingTime_df, HandlingRates_df, BusTime_df, Time_df)