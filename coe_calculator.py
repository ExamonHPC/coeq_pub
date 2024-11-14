import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# Impostazione della connessione API
with open("entsoe_key.json") as config_file:
    config = json.load(config_file)

client = EntsoePandasClient(api_key=config['ENTSOE_KEY'])

# Mappatura dei valori per ciascuna fonte di energia
energy_values = {
    "Coal": 1104,
    "Gas": 549,
    "Biomass": 230,
    "Geothermal": 38,
    "Solar": 27,
    "Hydropower": 11,
    "Hydropower_Reservoir": 374,
    "Wind": 13,
    "Oil": 1103,
    "Oil_Cons": 1103,
    "Nuclear": 5,
    "Battery": 374,
    "Waste": 0,
    "Other": 700,
    "HydroStorage": 30,
    'HydroStorage_Aggregated': 30,
    'WindOffshore': 13,
    'Lignite': 1104,
    'CoalDerivedGas': 1104,
    'Renewable': 1000
}
column_name_mapping = {
    'Biomass - Actual Aggregated': 'Biomass',
    'Fossil Gas - Actual Aggregated': 'Gas',
    'Fossil Oil - Actual Aggregated': 'Oil',
    'Fossil Oil - Actual Consumption': 'Oil_Cons',
    'Geothermal - Actual Aggregated': 'Geothermal',
    'Hydro Run-of-river and poundage - Actual Aggregated': 'Hydropower',
    'Hydro Water Reservoir - Actual Aggregated': 'Hydropower_Reservoir',
    'Other - Actual Aggregated': 'Other',
    'Solar - Actual Aggregated': 'Solar',
    'Waste - Actual Aggregated': 'Waste',
    'Wind Onshore - Actual Aggregated': 'Wind',
    'Fossil Hard coal - Actual Aggregated': 'Coal',
    'Hydro Pumped Storage - Actual Aggregated': 'HydroStorage_Aggregated',
    'Hydro Pumped Storage - Actual Consumption': 'HydroStorage',
    'Nuclear - Actual Aggregated': 'Nuclear',
    'Wind Offshore - Actual Aggregated': 'WindOffshore',
    'Fossil Brown coal/Lignite - Actual Aggregated': 'Lignite',
    'Fossil Coal-derived gas - Actual Aggregated': 'CoalDerivedGas',
    'Fossil Coal-derived gas': 'CoalDerivedGas',
    'Other renewable - Actual Aggregated': 'Renewable'
}

# Aggiornamenti specifici per ciascun paese
country_updates = {
    "france": {"Gas": 502, "Coal": 969, "Oil": 999, "Solar": 30, "Hydropower_Reservoir": 67},
    "switzerland": {"Nuclear": 12, "Coal": 820, "Wind": 11, "Solar": 30, "Hydropower": 24, 
                   "Hydropower_Reservoir": 68, "Gas": 490, "Oil": 650, "Other": 165},
    "austria": {"Coal": 1187, "Solar": 31, "Hydropower": 11, "Hydropower_Reservoir": 237, 
                "Battery": 237, "Gas": 528, "Oil": 1170},
    "slovenia": {"Coal": 1042, "Solar": 31, "Hydropower": 11, "Hydropower_Reservoir": 323, 
                 "Battery": 232, "Gas": 532, "Oil": 1170},
    "italyCentreNorth": {"Hydropower_Reservoir": 306, "Battery": 374}
}

def get_COE():
    # Inizializzazione dei valori per ciascun paese
    country_energy_values = {country: energy_values.copy() for country in country_updates}

    # Aggiornamento dei valori energetici per ciascun paese
    for country, updates in country_updates.items():
        country_energy_values[country].update(updates)

    # Creazione del dizionario che raccoglie i dati per ciascun paese
    countries_data = {country: country_energy_values[country] for country in country_energy_values}

    # Impostazione della fascia oraria
    current_time = datetime.utcnow()
    next_day_time = current_time + timedelta(days=1)
    start_date = current_time.strftime("%Y%m%d")
    end_date = next_day_time.strftime("%Y%m%d")
    start_timestamp = pd.Timestamp(start_date, tz='UTC')
    end_timestamp = pd.Timestamp(end_date, tz='UTC')

    country_code = 'IT_NORD'
    try:
        generation_data = client.query_generation(country_code, start=start_timestamp, end=end_timestamp)
        generation_df = pd.DataFrame(generation_data.iloc[-1:])
    except:
        print("API Error on getting last results")
        previous_day_time = current_time - timedelta(days=1)
        previous_date = previous_day_time.strftime("%Y%m%d")
        previous_timestamp = pd.Timestamp(previous_date, tz='UTC')
        generation_data = client.query_generation(country_code, start=previous_timestamp, end=start_timestamp)
        generation_df = pd.DataFrame(generation_data.iloc[-1:])

    generation_df.index = pd.to_datetime(generation_df.index)

    def format_mtu(index):
        start_time = index - pd.Timedelta(hours=1)
        end_time = index
        return f"{start_time.strftime('%d.%m.%Y %H:%M')} - {end_time.strftime('%d.%m.%Y %H:%M')} (UTC)"

    generation_df['MTU'] = generation_df.index.map(format_mtu)
    generation_df.reset_index(drop=True, inplace=True)
    generation_df.replace('n/e', np.nan, inplace=True)
    cleaned_df = generation_df.dropna(axis=1, how='all')
    cleaned_df = cleaned_df.replace(np.nan, 0)
    
    # Estrazione di Data e Ora
    if isinstance(cleaned_df.columns, pd.MultiIndex):
        print("MultiIndex")
        cleaned_df.columns = [' - '.join(col).strip() for col in cleaned_df.columns]
    print(cleaned_df.columns)
    if  "- Actual Aggregated" not in cleaned_df.columns[0]:
        print("Missing Actual")
        standardized_columns = {
            col: f"{col} - Actual Aggregated" for col in cleaned_df.columns if col not in ['Date', 'Time', 'MTU'] }
        cleaned_df.rename(columns=standardized_columns, inplace=True)
    cleaned_df.rename(columns=column_name_mapping, inplace=True)

    print(cleaned_df.columns)

    cleaned_df['Date'] = cleaned_df['MTU'].str.split(' - ').str[0].str.split(' ').str[0]
    cleaned_df['Time'] = cleaned_df['MTU'].str.split(' - ').str[0].str.split(' ').str[1]

    cleaned_df.drop(columns=['Area'], errors='ignore', inplace=True)

    # Order columns for output
    ordered_columns = ['Date', 'Time'] + [col for col in cleaned_df.columns if col not in ['Date', 'Time', 'MTU']]
    cleaned_df = cleaned_df[ordered_columns]

    # Calcolo della kWh totale e dell'intensità carbonica
    energy_sources = [col for col in cleaned_df.columns if col not in ['Date', 'Time', 'MTU']]
    print(energy_sources,cleaned_df.columns)
    cleaned_df['Total_kWh'] = cleaned_df[energy_sources].sum(axis=1)
    cleaned_df['Carbon_Intensity'] = cleaned_df[energy_sources].apply(lambda row: sum(row[source] * energy_values[source] for source in energy_sources) / cleaned_df['Total_kWh'], axis=1)

    return(cleaned_df['Carbon_Intensity'].values[0])
