import pandas as pd
import pandasql as ps

interval = 5
bus_seats_cnt = 80

data_folder_path = '../data/'
raw_data_folder_path = data_folder_path + 'raw_data/'
opt_data_folder_path = data_folder_path + 'opt_data/'

dat_file_name = opt_data_folder_path + 'aircraft.dat'

def create_dat(dat_file_name):
    with open(dat_file_name, 'w') as dat_file:
        HandlingRates_df = pd.read_csv(opt_data_folder_path + 'HandlingRates.csv')
        def write_param_to_data(dat_file, df, param_name):
            dat_file.write('param {param_name} := {param_value};\n'.format(param_name = param_name, param_value = int(df[param_name])))
        write_param_to_data(dat_file, HandlingRates_df, 'BusCost') 
        write_param_to_data(dat_file, HandlingRates_df, 'BusStandCost') 
        write_param_to_data(dat_file, HandlingRates_df, 'BridgeStandCost') 
        write_param_to_data(dat_file, HandlingRates_df, 'TaxingCost')

        dat_file.write('''
        load ../data/opt_data/Flights.csv  using = csv : 
            FLIGHTS = [ID] 
            FlightTime = FlightTime 
            FlightID = FlightID 
            FlightAD = FlightAD 
            FlightTerminal = FlightTerminal 
            FlightClass = FlightClass 
            BusRequired = BusRequired 
            FlightTime = FlightTime
        ; \n''')
        dat_file.write('''
        load ../data/opt_data/Stands.csv  using = csv : 
            STANDS = [ID] 
            StandNum = StandNum 
            JetBridgeArrival = JetBridgeArrival 
            JetBridgeDeparture = JetBridgeDeparture 
            StandTerminal = StandTerminal 
            TaxingTime = TaxingTime 
        ; \n''')
        dat_file.write('''
        load ../data/opt_data/HandlingTime.csv  using = csv : 
            CLASSES = [ID] 
            BridgeHandlingTime = BridgeHandlingTime 
            BusHandlingTime = BusHandlingTime 
        ; \n''')
        dat_file.write('''
        load ../data/opt_data/BusTime.csv  using = csv : 
            [StandID TerminalID] 
            BusTime = BusTime 
        ; \n''')
        dat_file.write('''
        load ../data/opt_data/Time.csv 
        using = csv format = set : 
            TIME
        ; \n''')
        dat_file.write('''
        load ../data/opt_data/Modes.csv 
        using = csv format = set : 
            MODES
        ; \n''')
create_dat(dat_file_name)