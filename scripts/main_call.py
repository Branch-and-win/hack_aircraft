from hack_aircraft import *

from pyomo.opt import SolverFactory
import pandas as pd
import pandasql as ps

# Считывание данных по расписанию A/D ВС
Timetable_df       = pd.read_csv(raw_data_folder_path + 'Timetable_private.csv')

solver_nm = 'cbc'

dat_file_nm = '../data/opt_data/aircraft_v1.dat'
results_file_nm = '../data/opt_data/out.yml'
results_file = open(results_file_nm, "w")

# Создание instance для асбтрактной модели
instance = model.create_instance(dat_file_nm)

# Определение солвера и настроек
Solver = SolverFactory(solver_nm)
if solver_nm == 'cbc':
    Solver.options['second' ] = 5400
    Solver.options['allowableGap' ] = 0.05
SolverResults = Solver.solve(instance, tee=True)

# Вывод результата
pd.set_option("display.max_rows", None, "display.max_columns", None)

pd.set_option("display.max_rows", None, "display.max_columns", None)
OutputResults = pd.DataFrame(data=[(f,s,instance.z[f,s].value) for f,s in instance.FLIGHTS_STANDS if instance.z[f,s].value > 0.01],
    index=list(instance.FLIGHTS_STANDS), columns = ['my_result.csv', 'FLIGHT_ID', 'STAND_ID'])

Timetable_df.drop(columns = ["Aircraft_Stand"], inplace = True)    

sqlcode = '''
        select A.*, B.STAND_ID as Aircraft_Stand
        from Timetable_df A
        left join OutputResults B
        on A.FLIGHT_ID = B.FLIGHT_ID
        '''
result_df = ps.sqldf(sqlcode,locals())

result_df.to_csv('../data/opt_data/result.csv', index = False)

results_file.close()