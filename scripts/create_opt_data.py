import pandas as pd
import pandasql as ps
import random
import numpy as np

interval = 1
time_interval = 5
bus_seats_cnt = 80

data_folder_path = '../data/'
raw_data_folder_path = data_folder_path + 'raw_data/'
opt_data_folder_path = data_folder_path + 'opt_data/'

# Считывание данных по МС ВС
AircraftStands_df  = pd.read_csv(raw_data_folder_path + 'Aircraft_Stands_Private.csv')
# Считывание данных по классам ВС
AirCraftClasses_df = pd.read_csv(raw_data_folder_path + 'Aircraft_Classes_Private.csv')
# Считывание данных по расписанию A/D ВС
Timetable_df       = pd.read_csv(raw_data_folder_path + 'Timetable_private.csv')
# Считывание данных по времени обсуживания ВС
HandlingTime_df    = pd.read_csv(raw_data_folder_path + 'Handling_Time_Private.csv')
# Различные косты для модели
HandlingRates_df    = pd.read_csv(raw_data_folder_path + 'Handling_Rates_Private.csv')

# Создание агрегированных стоянок
def create_aggr_stands(AircraftStands_df):
    columns_list = list(AircraftStands_df.columns.values)
    char_columns_list = columns_list.copy()
    AircraftStands_df['ID'] = AircraftStands_df['Aircraft_Stand']
    AircraftStands_df['Terminal'] = AircraftStands_df['Terminal'].fillna(-1).astype(int)
    return AircraftStands_df
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
    # return ACClass_df.set_index('Aircraft_Class').T
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
    times = [i_time for i_time in range(int(-60 / interval), int(60+24 * 60 / interval))]
    return pd.DataFrame(times, columns =['ID'])
Time_df = create_time(interval = time_interval)

# Преобразование дат в id интервала
def time_to_interval(df, interval, time_col, interval_col, drop = False):
    df[time_col] = df[time_col].astype('datetime64[ns]', copy=False)
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
        , 'Taxiing_Time'           : 'TaxingTime'}
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



# Вычисление костов

# Выходная таблица [FLIGHT_ID, STAND_ID] COST BRIDGE_FLAG
Cost_nm = opt_data_folder_path + 'Cost.csv'

# Считывание данных по расписанию A/D ВС
Flights_df  = pd.read_csv(opt_data_folder_path + 'Flights.csv')
# Считывание данных по времени обсуживания ВС
HandlingTime_df    = pd.read_csv(opt_data_folder_path + 'HandlingTime.csv')
# Различные косты для модели
HandlingRates_df    = pd.read_csv(opt_data_folder_path + 'HandlingRates.csv')
# Считывание данных по классам ВС
Stands_df = pd.read_csv(opt_data_folder_path + 'Stands.csv')
BusTime_df = pd.read_csv(opt_data_folder_path + 'BusTime.csv')

sqlcode = '''
    with FLIGHT_STAND_COST as
    (
        with cost as
        (
            select 
                F.ID as FLIGHT_ID,
                S.ID as STAND_ID,
                HR.TaxingCost * S.TaxingTime +
                    (CASE 
                        WHEN F.FlightID = S.JetBridgeArrival and F.FlightTerminal = S.StandTerminal THEN 
                            HR.BridgeStandCost * HT.BridgeHandlingTime
                        WHEN S.StandTerminal > 0 THEN
                            HR.BridgeStandCost * HT.BusHandlingTime + F.BusRequired * HR.BusCost * BT.BusTime
                        ELSE
                            HR.BusStandCost * HT.BusHandlingTime + F.BusRequired * HR.BusCost * BT.BusTime
                    END) as COST,
                (CASE WHEN F.FlightID = S.JetBridgeArrival and F.FlightTerminal = S.StandTerminal THEN 1 ELSE 0 END) as BRIDGE_FLAG,
                F.FlightAD,
                F.FlightClass,
                F.FlightTime,
                S.TaxingTime
            from Flights_df F
            left join HandlingTime_df HT
            on F.FlightClass = HT.ID
            cross join HandlingRates_df HR
            cross join Stands_df S
            left join BusTime_df BT
            on S.ID = BT.StandID
                and F.FlightTerminal = BT.TerminalID
        )
        select *
            , ROW_NUMBER() OVER(PARTITION BY T.FLIGHT_ID ORDER BY COST) as rn
        from cost T
    )
    select 
        T.FLIGHT_ID,
        T.STAND_ID,
        T.COST,
        T.BRIDGE_FLAG,
        T.FlightAD,
        T.FlightClass,
        T.FlightTime,
        T.TaxingTime
    from FLIGHT_STAND_COST T
    where rn <= 90
    '''
ps.sqldf(sqlcode,locals()).to_csv(Cost_nm, index = False)



# Вычисление занятости STAND_ID

# Выходная таблица [FLIGHT_ID, STAND_ID] COST BRIDGE_FLAG
SumBridge_nm    = opt_data_folder_path + 'SumBridge.csv'
SumBus_nm       = opt_data_folder_path + 'SumBus.csv'

# Считывание данных по времени обсуживания ВС
HandlingTime_df    = pd.read_csv(opt_data_folder_path + 'HandlingTime.csv')
# Различные косты для модели
HandlingRates_df    = pd.read_csv(opt_data_folder_path + 'HandlingRates.csv')
# Считывание данных по классам ВС
Stands_df = pd.read_csv(opt_data_folder_path + 'Stands.csv')
# Считывание данных по интервалам времени
Time_df = pd.read_csv(opt_data_folder_path + 'Time.csv')

# Считывание данных по интервалам времени
Cost_df = pd.read_csv(opt_data_folder_path + 'Cost.csv')

def calc_sum(
    out_nm,
    interval,
    BRIDGE_FLAG,
    HandlingStartTimeA,
    HandlingEndTimeA,
    HandlingStartTimeD,
    HandlingEndTimeD,
    HandlingTime_df=HandlingTime_df,
    Stands_df=Stands_df,
    Time_df=Time_df,
    Cost_df=Cost_df
):
    sqlcode = '''
        with HandlingTimes as
        (
            select 
                C.FLIGHT_ID,
                C.STAND_ID,
                T.ID as TIME_ID,
                cast ( (CASE 
                    WHEN C.FlightAD = 'A' THEN {HandlingStartTimeA} 
                    ELSE {HandlingStartTimeD} 
                END) AS float) / {interval} as HandlingStartTime,
                cast ( (CASE 
                    WHEN C.FlightAD = 'A' THEN {HandlingEndTimeA} 
                    ELSE {HandlingEndTimeD} 
                END) AS float) / {interval} as HandlingEndTime,
                C.BRIDGE_FLAG,
                C.TaxingTime % 5 mod_TT,
                C.FlightAD
            from Cost_df C 
            left join HandlingTime_df HT
            on C.FlightClass = HT.ID
            left join Time_df T
            on (C.FlightAD = 'A' and T.ID >= {HandlingStartTimeA} / {interval} - 1 and T.ID <= {HandlingEndTimeA} / {interval} + 1)
                or (C.FlightAD = 'D' and T.ID >= {HandlingStartTimeD} / {interval} - 1 and T.ID <= {HandlingEndTimeD} / {interval} + 1)
        )
        select 
            T.FLIGHT_ID,
            T.STAND_ID,
            T.TIME_ID
        from HandlingTimes T
        where 1=1
            and T.BRIDGE_FLAG = {BRIDGE_FLAG} and
            (
                (mod_TT < 3 and T.TIME_ID >= cast ( T.HandlingStartTime as int ) )
                or
                (mod_TT >= 3  and T.TIME_ID >= cast ( T.HandlingStartTime as int ) + ( T.HandlingStartTime  > cast ( T.HandlingStartTime  as int )) )
            )
            and T.TIME_ID <  cast ( T.HandlingEndTime as int ) + ( T.HandlingEndTime  > cast ( T.HandlingEndTime  as int ))
        '''.format(interval = interval
                , BRIDGE_FLAG = BRIDGE_FLAG
                , HandlingStartTimeA = HandlingStartTimeA
                , HandlingEndTimeA   = HandlingEndTimeA
                , HandlingStartTimeD = HandlingStartTimeD
                , HandlingEndTimeD   = HandlingEndTimeD)
    result_df = ps.sqldf(sqlcode,locals())
    result_df.to_csv(out_nm, index = False)

calc_sum(
    out_nm = SumBridge_nm,
    interval = interval,
    BRIDGE_FLAG = 1,
    HandlingStartTimeA = '(C.FlightTime + C.TaxingTime)',
    HandlingEndTimeA   = '(C.FlightTime + C.TaxingTime + HT.BridgeHandlingTime)',
    HandlingStartTimeD = '(C.FlightTime - C.TaxingTime - HT.BridgeHandlingTime)',
    HandlingEndTimeD   = '(C.FlightTime - C.TaxingTime)'
)
calc_sum(
    out_nm = SumBus_nm,
    interval = interval,
    BRIDGE_FLAG = 0,
    HandlingStartTimeA = '(C.FlightTime + C.TaxingTime)',
    HandlingEndTimeA   = '(C.FlightTime + C.TaxingTime + HT.BusHandlingTime)',
    HandlingStartTimeD = '(C.FlightTime - C.TaxingTime - HT.BusHandlingTime)',
    HandlingEndTimeD   = '(C.FlightTime - C.TaxingTime)'
)



# Формирование множеств для ограничений в оптимизации

FeasibleBridge_nm = opt_data_folder_path + 'FeasibleBridge.csv'
FeasibleBusBridge_nm = opt_data_folder_path + 'FeasibleBusBridge.csv'

FeasibleBridgeWide_nm = opt_data_folder_path + 'FeasibleBridgeWide.csv'
FeasibleWide_nm = opt_data_folder_path + 'FeasibleWide.csv'

# Считывание данных по телетрапам
SumBridge_df = pd.read_csv(opt_data_folder_path + 'SumBridge.csv')
SumBus_df = pd.read_csv(opt_data_folder_path + 'SumBus.csv')
Flights_df = pd.read_csv(opt_data_folder_path + 'Flights.csv')

sqlcode = '''
    with FeasibleWide as
    (
        with FeasibleBridgeBus as
        (
            select 
                C.STAND_ID,
                C.TIME_ID,
                C.FLIGHT_ID
            from SumBridge_df C 
            UNION ALL
            select 
                C.STAND_ID,
                C.TIME_ID,
                C.FLIGHT_ID
            from SumBus_df C 
        )
        select distinct
            FBB.STAND_ID,
            FBB.TIME_ID
        from FeasibleBridgeBus FBB
        left join Flights_df F
        on F.ID = FBB.FLIGHT_ID
        where F.FlightClass = "Wide_Body"
    )
    select t1.STAND_ID, t1.TIME_ID
    from FeasibleWide t1
    left join FeasibleWide t2
    on t1.STAND_ID = t2.STAND_ID - 1
        and t1.TIME_ID = t2.TIME_ID
    where t2.STAND_ID is not null
    '''
    
ps.sqldf(sqlcode,locals()).to_csv(FeasibleBridgeWide_nm, index = False)


sqlcode = '''
    with FeasibleBridgeBus as
        (
        select 
            C.STAND_ID,
            C.TIME_ID,
            C.FLIGHT_ID
        from SumBridge_df C 
        UNION ALL
        select 
            C.STAND_ID,
            C.TIME_ID,
            C.FLIGHT_ID
        from SumBus_df C 
        )
    select distinct
        FBB.STAND_ID,
        FBB.TIME_ID,
        FBB.FLIGHT_ID
    from FeasibleBridgeBus FBB
    left join Flights_df F
    on F.ID = FBB.FLIGHT_ID
    where F.FlightClass = "Wide_Body"
    '''
    
ps.sqldf(sqlcode,locals()).to_csv(FeasibleWide_nm, index = False)


sqlcode = '''
    with FeasibleBridge as
    (
        select 
            C.STAND_ID,
            C.TIME_ID
        from SumBridge_df C 
        group by
            C.STAND_ID,
            C.TIME_ID
        having count(C.FLIGHT_ID) > 0
    )
    select t1.STAND_ID, t1.TIME_ID
    from FeasibleBridge t1
    left join FeasibleBridge t2
    on t1.STAND_ID = t2.STAND_ID - 1
        and t1.TIME_ID = t2.TIME_ID
    where t2.STAND_ID is not null
    '''
    
ps.sqldf(sqlcode,locals()).to_csv(FeasibleBridge_nm, index = False)




sqlcode = '''
    with FeasibleBridgeBus as
    (
        with FeasibleBridge as
        (
            select 
                C.STAND_ID,
                C.TIME_ID,
                count(C.FLIGHT_ID) as cnt
            from SumBridge_df C 
            group by
                C.STAND_ID,
                C.TIME_ID
            having count(C.FLIGHT_ID) > 0
        ),
        FeasibleBus as
        (
            select 
                C.STAND_ID,
                C.TIME_ID,
                count(C.FLIGHT_ID) as cnt
            from SumBus_df C 
            group by
                C.STAND_ID,
                C.TIME_ID
            having count(C.FLIGHT_ID) > 0
        )
        select t1.STAND_ID, t1.TIME_ID
        from FeasibleBridge t1
        left join FeasibleBus t2
        on t1.STAND_ID = t2.STAND_ID and t1.TIME_ID = t2.TIME_ID
        where coalesce(t1.cnt,0)+coalesce(t2.cnt,0) > 1
        UNION ALL
        select t1.STAND_ID, t1.TIME_ID
        from FeasibleBus t1
        left join FeasibleBridge t2
        on t1.STAND_ID = t2.STAND_ID and t1.TIME_ID = t2.TIME_ID
        where coalesce(t1.cnt,0)+coalesce(t2.cnt,0) > 1
    )
    select distinct t1.STAND_ID, t1.TIME_ID
    from FeasibleBridgeBus t1
    '''
    
ps.sqldf(sqlcode,locals()).to_csv(FeasibleBusBridge_nm, index = False)
