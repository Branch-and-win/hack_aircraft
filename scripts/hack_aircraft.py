import pyomo.environ as pyo
import math
import pandas as pd


data_folder_path = '../data/'
raw_data_folder_path = data_folder_path + 'raw_data/'
opt_data_folder_path = data_folder_path + 'opt_data/'

FLIGHTS_STANDS_TIMES_BRIDGE_df    = pd.read_csv(opt_data_folder_path + 'SumBridge.csv')
STANDS_TIMES_BRIDGE_dict = {idx: group['FLIGHT_ID'].tolist() for idx, group in FLIGHTS_STANDS_TIMES_BRIDGE_df.groupby(['STAND_ID','TIME_ID'])}
STANDS_TIMES_BRIDGE_dict_keys = list(STANDS_TIMES_BRIDGE_dict.keys())

FLIGHTS_STANDS_TIMES_BUS_df    = pd.read_csv(opt_data_folder_path + 'SumBus.csv')
STANDS_TIMES_BUS_dict = {idx: group['FLIGHT_ID'].tolist() for idx, group in FLIGHTS_STANDS_TIMES_BUS_df.groupby(['STAND_ID','TIME_ID'])}
STANDS_TIMES_BUS_dict_keys = list(STANDS_TIMES_BUS_dict.keys())

FeasibleWide_df    = pd.read_csv(opt_data_folder_path + 'FeasibleWide.csv')
FeasibleWide_dict = {idx: group['FLIGHT_ID'].tolist() for idx, group in FeasibleWide_df.groupby(['STAND_ID','TIME_ID'])}
FeasibleWide_dict_keys = list(FeasibleWide_dict.keys())




model = pyo.AbstractModel()


#Множество рейсов
model.FLIGHTS = pyo.Set()


#Момент прилета рейса
model.FlightTime = pyo.Param(model.FLIGHTS)

#Номер терминала рейса
model.FlightTerminal = pyo.Param(model.FLIGHTS)

#Класс ВС
model.FlightClass = pyo.Param(model.FLIGHTS)

#Метка прилет/вылет (A или D)
model.FlightAD = pyo.Param(model.FLIGHTS)


#Множество отобранных пар рейс - место стоянки
model.FLIGHTS_STANDS = pyo.Set()

#Суммарная затраты на размещение рейса на стоянке
model.Cost = pyo.Param(model.FLIGHTS_STANDS)

#Индикатор использования телетрапа для пары рейс - место стоянки
model.BridgeFlag = pyo.Param(model.FLIGHTS_STANDS)


#Моменты времени, когда стоянка занята для пары рейс - стоянка (с автобусами)
model.FLIGHTS_STANDS_TIMES_BUS = pyo.Set()

#Моменты времени, когда стоянка занята для пары рейс - стоянка (с телетрапами)
model.FLIGHTS_STANDS_TIMES_BRIDGE = pyo.Set()



#Множество терминалов
model.TERMINALS = pyo.RangeSet(1,5)


#Множество агрегированных стоянок
model.STANDS = pyo.Set()

#Принадлежность к терминалу
model.StandTerminal = pyo.Param(model.STANDS, default = 0)

#Время руления от ВПП до стоянки
model.TaxingTime = pyo.Param(model.STANDS)


#Множество классов ВС
model.CLASSES = pyo.Set()

#Время на обслуживание класса ВС на контактном МС
model.BridgeHandlingTime = pyo.Param(model.CLASSES)

#Время на обслуживание класса ВС на удаленном МС
model.BusHandlingTime = pyo.Param(model.CLASSES)


#Множество моментов времени 
model.TIME = pyo.Set()


#Уровень детализации ограничений (в минутах)
interval = 5


## Переменные решения


#Индикатор использования  места стоянки для рейса
model.z = pyo.Var(model.FLIGHTS_STANDS, within = pyo.Binary, initialize = 0)


## Неявные переменные решения

##  Ограничения

#Определяем множество для ограничения на соседние широкофюзеляжные ВС (оставляем только те моменты времени, когда существует возможность нарушить ограничение)
def wide_max_set_filter(m,s,t):
    return m.StandTerminal[s] > 0 and len([(f1,s1,t1) for (f1,s1,t1) in m.FLIGHTS_STANDS_TIMES_BRIDGE if s1 == s and t1 == t]) > 0

# model.WIDE_MAX_SET = pyo.Set(initialize = model.STANDS * model.TIME, filter = wide_max_set_filter)

#Нельзя иметь рядом два широкофюзеляжных ВС с телетрапом
def wide_max_rule(m,s,t):
    return sum(m.z[f,s] for f in FeasibleWide_dict[s,t]) \
            + sum(m.z[f,s+1] for f in FeasibleWide_dict[s+1,t]) <= 1

model.WIDE_FEASIBLE_SET = pyo.Set()
model.wide_max = pyo.Constraint(model.WIDE_FEASIBLE_SET, rule = wide_max_rule)


#В каждый момент времени не более одного рейса на стоянке
def stand_capacity_rule(m,s,t):
    if (s,t) not in STANDS_TIMES_BRIDGE_dict_keys:
        return sum(m.z[f,s] for f in STANDS_TIMES_BUS_dict[s,t])  <= 1
    if (s,t) not in STANDS_TIMES_BUS_dict_keys:
        return sum(m.z[f,s] for f in STANDS_TIMES_BRIDGE_dict[s,t])  <= 1
    return sum(m.z[f,s] for f in STANDS_TIMES_BUS_dict[s,t]) \
            + sum(m.z[f,s] for f in STANDS_TIMES_BRIDGE_dict[s,t])  <= 1

model.BUS_BRIDGE_FEASIBLE_SET = pyo.Set()
model.stand_capacity = pyo.Constraint(model.BUS_BRIDGE_FEASIBLE_SET, rule = stand_capacity_rule)

#Каждому рейсу необходимо выделить ровно одну стоянку
def one_stand_rule(m,f):
    return sum(m.z[f,s] for (f1,s) in m.FLIGHTS_STANDS if f1 == f) == 1

model.stand_rule = pyo.Constraint(model.FLIGHTS, rule = one_stand_rule)


## Целевая функция - минимизация суммарных затрат


def ObjCosts(m):
    return sum(m.z[f,s] * m.Cost[f,s] for (f,s) in m.FLIGHTS_STANDS)

model.OBJ = pyo.Objective(rule = ObjCosts, sense = pyo.minimize)
