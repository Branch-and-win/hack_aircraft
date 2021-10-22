import pyomo.environ as pyo

model = pyo.AbstractModel()


#Множество рейсов
model.FLIGHTS = pyo.Set()


#Момент прилета рейса
model.FlightTime = pyo.Param(model.FLIGHTS)

#Внутренний или международный рейс (I или D)
model.FlightID = pyo.Param(model.FLIGHTS)

#Метка прилет/вылет (A или D)
model.FlightAD = pyo.Param(model.FLIGHTS)

#Номер терминала рейса
model.FlightTerminal = pyo.Param(model.FLIGHTS)

#Класс ВС
model.FlightClass = pyo.Param(model.FLIGHTS)

#Количество необходимых автобусов для перевозки всех пассажиров рейса
model.BusRequired = pyo.Param(model.FLIGHTS)


#Множество терминалов
model.TERMINALS = pyo.RangeSet(1,5)


#Множество агрегированных стоянок
model.STANDS = pyo.Set()


#Количество агрегированных стоянок
model.StandNum = pyo.Param(model.STANDS)

#Возможность использования телетрапа на прилете
model.JetBridgeArrival = pyo.Param(model.STANDS)

#Возможность использования телетрапа на вылете
model.JetBridgeDeparture = pyo.Param(model.STANDS)

#Принадлежность к терминалу
model.StandTerminal = pyo.Param(model.STANDS, default = 0)

#Время руления от ВПП до стоянки
model.TaxingTime = pyo.Param(model.STANDS)

#Время движения автобуса от терминала до места стоянки
model.BusTime = pyo.Param(model.STANDS, model.TERMINALS)


#Множество классов ВС
model.CLASSES = pyo.Set()


#Время на обслуживание класса ВС на контактном МС
model.BridgeHandlingTime = pyo.Param(model.CLASSES)

#Время на обслуживание класса ВС на удаленном МС
model.BusHandlingTime = pyo.Param(model.CLASSES)


#Множество моментов времени (с детализацией до 5 минут)
model.TIME = pyo.Set()


#Стоимость использования 1 минуты автобуса
model.BusCost = pyo.Param()

#Стоимость 1 минуты руления от ВПП до МС
model.TaxingCost = pyo.Param()

#Стоимость 1 минуты стоянки на МС с телетрапом
model.BridgeStandCost = pyo.Param()

#Стоимость 1 минуты стоянки на МС без телетрапа
model.BusStandCost = pyo.Param()


# Режимы использования МС (Автобус или телетрап)
model.MODES = pyo.Set()


## Переменные решения

#Индикатор использования агрегированного места для рейса и выбор механизма (телетрап или автобус)
model.z = pyo.Var(model.FLIGHTS, model.STANDS,  model.MODES, within = pyo.Binary, initialize = 0)


## Неявные переменные решения

#Количество занятых мест с использованием телетрапа в момент t
def WideBridgeCount(m,s,t):
    arrival_sum_bridge = sum(m.z[f,s,'bridge'] for f in m.FLIGHTS if m.FlightAD[f] == 'A' and m.FlightClass[f] == 'Wide_Body' and \
        t in range(m.FlightTime[f] + int(m.TaxingTime[s]/5), m.FlightTime[f] + int((m.TaxingTime[s] + m.BridgeHandlingTime[m.FlightClass[f]])/5)))

    departure_sum_bridge = sum(m.z[f,s,'bridge'] for f in m.FLIGHTS if m.FlightAD[f] == 'D' and m.FlightClass[f] == 'Wide_Body' and \
        t in range(m.FlightTime[f] - int((m.TaxingTime[s] + m.BridgeHandlingTime[m.FlightClass[f]])/5), m.FlightTime[f] - int(m.TaxingTime[s]/5)))

    return arrival_sum_bridge  + departure_sum_bridge 

#Количество занятых мест на агрегированной стоянке в момент t
def StandTimeCount(m,s,t):
    arrival_sum_bridge = sum(m.z[f,s,'bridge'] for f in m.FLIGHTS if m.FlightAD[f] == 'A' and \
        t in range(m.FlightTime[f] + int(m.TaxingTime[s]/5), m.FlightTime[f] + int((m.TaxingTime[s] + m.BridgeHandlingTime[m.FlightClass[f]])/5)))

    arrival_sum_bus = sum(m.z[f,s,'bus'] for f in m.FLIGHTS if m.FlightAD[f] == 'A' and \
        t in range(m.FlightTime[f] + int(m.TaxingTime[s]/5), (m.FlightTime[f] + int((m.TaxingTime[s] + m.BusHandlingTime[m.FlightClass[f]])/5))))

    departure_sum_bridge = sum(m.z[f,s,'bridge'] for f in m.FLIGHTS if m.FlightAD[f] == 'D' and \
        t in range(m.FlightTime[f] - int((m.TaxingTime[s] + m.BridgeHandlingTime[m.FlightClass[f]])/5), m.FlightTime[f] - int(m.TaxingTime[s]/5)))

    departure_sum_bus = sum(m.z[f,s,'bus'] for f in m.FLIGHTS if m.FlightAD[f] == 'D' and \
        t in range(m.FlightTime[f] - int((m.TaxingTime[s] + m.BusHandlingTime[m.FlightClass[f]])/5), m.FlightTime[f] - int(m.TaxingTime[s]/5)))

    return arrival_sum_bus + departure_sum_bus + arrival_sum_bridge + departure_sum_bridge


#Суммарная стоимость перевозки пассажиров на автобусе
def BusCosts(m):
    return sum(m.z[f,s,'bus'] * m.BusCost * m.BusRequired[f] * m.BusTime[s,m.FlightTerminal[f]] \
           for f in m.FLIGHTS for s in m.STANDS)

#Суммарная стоимость обслуживания на местах стоянки
def StandCosts(m):
    BusCosts = sum(m.z[f,s,'bus'] * m.BusStandCost * m.BusHandlingTime[m.FlightClass[f]] for f in m.FLIGHTS for s in m.STANDS if m.StandTerminal[s] == -1)
    
    BridgeCosts = sum(m.z[f,s,'bridge'] * m.BridgeStandCost * m.BridgeHandlingTime[m.FlightClass[f]] + 
                   m.z[f,s,'bus'] * m.BridgeStandCost * m.BusHandlingTime[m.FlightClass[f]] for f in m.FLIGHTS for s in m.STANDS if m.StandTerminal[s] > 0)

    #BusCosts = sum(m.z[f,s,'bus'] * m.BusStandCost * m.BusHandlingTime[m.FlightClass[f]] for f in m.FLIGHTS for s in m.STANDS)
    #BridgeCosts = sum(m.z[f,s,'bridge'] * m.BridgeStandCost * m.BridgeHandlingTime[m.FlightClass[f]] for f in m.FLIGHTS for s in m.STANDS)
    return BusCosts + BridgeCosts

#Суммарная стоимость руления от ВВП до мест стоянки
def TaxingCosts(m):
    return sum(m.z[f,s,mode] * m.TaxingCost * m.TaxingTime[s] for f in m.FLIGHTS for s in m.STANDS for mode in m.MODES)


##  Ограничения


#Запрет использования телетрапа
def bridge_use_rule(m,f,s):
    if (m.StandTerminal[s] != m.FlightTerminal[f]) or ((m.FlightAD[f] == 'A' and m.FlightID[f] != m.JetBridgeArrival[s]) and \
            (m.FlightAD[f] == 'D' and m.FlightID[f] != m.JetBridgeDeparture[s])):
        return m.z[f,s,'bridge'] == 0
    else:
        return pyo.Constraint.Skip

model.bridge_use = pyo.Constraint(model.FLIGHTS, model.STANDS, rule = bridge_use_rule)

#Запрет использования автобуса
def bus_use_rule(m,f,s):
    if (m.StandTerminal[s] == m.FlightTerminal[f]) and ((m.FlightAD[f] == 'A' and m.FlightID[f] == m.JetBridgeArrival[s]) or \
            (m.FlightAD[f] == 'D' and m.FlightID[f] == m.JetBridgeDeparture[s])):
        return m.z[f,s,'bus'] == 0
    else:
        return pyo.Constraint.Skip

model.bus_use = pyo.Constraint(model.FLIGHTS, model.STANDS, rule = bus_use_rule)

#Определяем множество для ограничения на соседние широкофюзеляжные ВС (оставляем только те моменты времени, когда существует возможность нарушить ограничение)
def wide_max_set_filter(m,s,t):
    return len([f for f in m.FLIGHTS if m.FlightAD[f] == 'A' and m.FlightClass[f] == 'Wide_Body' and \
        t in range(m.FlightTime[f] + int(m.TaxingTime[s]/5), m.FlightTime[f] + int((m.TaxingTime[s] + m.BridgeHandlingTime[m.FlightClass[f]])/5))]) + \
        len([f for f in m.FLIGHTS if m.FlightAD[f] == 'D' and m.FlightClass[f] == 'Wide_Body' and \
        t in range(m.FlightTime[f] - int((m.TaxingTime[s] + m.BridgeHandlingTime[m.FlightClass[f]])/5), m.FlightTime[f] - int(m.TaxingTime[s]/5))]) \
        > (m.StandNum[s] // 2) + (m.StandNum[s] % 2) 

model.WIDE_MAX_SET = pyo.Set(initialize = model.STANDS * model.TIME, filter = wide_max_set_filter)

#Нельзя иметь рядом два широкофюзеляжных ВС с телетрапом
def wide_max_rule(m,s,t):
    return WideBridgeCount(m,s,t) <= (m.StandNum[s] // 2) + (m.StandNum[s] % 2) 

model.wide_max = pyo.Constraint(model.WIDE_MAX_SET, rule = wide_max_rule)


#В каждый момент времени ограниченное число мест на агрегированной стоянке
def stand_capacity_rule(m,s,t):
    return StandTimeCount(m,s,t) <= m.StandNum[s]

model.stand_capacity = pyo.Constraint(model.STANDS, model.TIME, rule = stand_capacity_rule)

#Каждому рейсу необходимо выделить ровно одну стоянку
def one_stand_rule(m,f):
    return sum(m.z[f,s,mode] for s in m.STANDS for mode in m.MODES) == 1

model.stand_rule = pyo.Constraint(model.FLIGHTS, rule = one_stand_rule)


## Целевая функция - минимизация суммарных затрат


def ObjCosts(m):
    return BusCosts(m) + StandCosts(m) + TaxingCosts(m) 

model.OBJ = pyo.Objective(rule = ObjCosts, sense=pyo.minimize)




