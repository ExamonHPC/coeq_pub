#!/home/examon/.venv/bin/python
import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# Load API key
with open("entsoe_key.json") as config_file:
    config = json.load(config_file)

client = EntsoePandasClient(api_key=config['ENTSOE_KEY'])

# Define mappings and energy values
energy_values = {
    "Coal": 1104, "Gas": 549, "Biomass": 230, "Geothermal": 38, "Solar": 27, "Hydropower": 11,
    "Hydropower_Reservoir": 374, "Wind": 13, "Oil": 1103, "Oil_Cons": 1103, "Nuclear": 5,
    "Battery": 374, "Waste": 0, "Other": 700, "HydroStorage": 30, "HydroStorage_Aggregated": 30,
    "WindOffshore": 13, "Lignite": 1104, "CoalDerivedGas": 1104, "Renewable": 1000
}

column_name_mapping = {
    'Biomass - Actual Aggregated': 'Biomass', 'Fossil Gas - Actual Aggregated': 'Gas',
    'Fossil Oil - Actual Aggregated': 'Oil', 'Fossil Oil - Actual Consumption': 'Oil_Cons',
    'Geothermal - Actual Aggregated': 'Geothermal', 'Hydro Run-of-river and poundage - Actual Aggregated': 'Hydropower',
    'Hydro Water Reservoir - Actual Aggregated': 'Hydropower_Reservoir', 'Other - Actual Aggregated': 'Other',
    'Solar - Actual Aggregated': 'Solar', 'Waste - Actual Aggregated': 'Waste',
    'Wind Onshore - Actual Aggregated': 'Wind', 'Fossil Hard coal - Actual Aggregated': 'Coal',
    'Hydro Pumped Storage - Actual Aggregated': 'HydroStorage_Aggregated',
    'Hydro Pumped Storage - Actual Consumption': 'HydroStorage', 'Nuclear - Actual Aggregated': 'Nuclear',
    'Wind Offshore - Actual Aggregated': 'WindOffshore', 'Fossil Brown coal/Lignite - Actual Aggregated': 'Lignite',
    'Fossil Coal-derived gas - Actual Aggregated': 'CoalDerivedGas', 'Other renewable - Actual Aggregated': 'Renewable'
}

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
    # Define time range
    current_time = datetime.utcnow()
    next_day_time = current_time + timedelta(days=1)
    start_timestamp = pd.Timestamp(current_time.strftime("%Y%m%d"), tz='UTC')
    end_timestamp = pd.Timestamp(next_day_time.strftime("%Y%m%d"), tz='UTC')

    country_code = 'IT_NORD'
    try:
        generation_data = client.query_generation(country_code, start=start_timestamp, end=end_timestamp)
        generation_df = pd.DataFrame(generation_data.iloc[-1:])
    except:
        print("API Error; retrying with the previous days data.")
        previous_timestamp = start_timestamp - timedelta(days=1)
        generation_data = client.query_generation(country_code, start=previous_timestamp, end=start_timestamp)
        generation_df = pd.DataFrame(generation_data.iloc[-1:])

    generation_df.index = pd.to_datetime(generation_df.index)
    generation_df.replace('n/e', np.nan, inplace=True)

    # Handle MultiIndex columns if present
    if isinstance(generation_df.columns, pd.MultiIndex):
        generation_df.columns = [' - '.join(col).strip() for col in generation_df.columns]

    # Standardize column names to ensure consistency with column_name_mapping
    standardized_columns = {
        col: f"{col} - Actual Aggregated" if col in column_name_mapping.values() else col
        for col in generation_df.columns
    }
    generation_df.rename(columns=standardized_columns, inplace=True)

    # Apply final renaming
    generation_df.rename(columns=column_name_mapping, inplace=True)

    # Format MTU column and extract Date/Time
    def format_mtu(index):
        start_time = index - pd.Timedelta(hours=1)
        return f"{start_time.strftime('%d.%m.%Y %H:%M')} - {index.strftime('%d.%m.%Y %H:%M')} (UTC)"

    generation_df['MTU'] = generation_df.index.map(format_mtu)
    generation_df['Date'] = generation_df['MTU'].str.split(' - ').str[0].str.split(' ').str[0]
    generation_df['Time'] = generation_df['MTU'].str.split(' - ').str[0].str.split(' ').str[1]



    # Drop unnecessary columns
    generation_df.drop(columns=['Area'], errors='ignore', inplace=True)



    # Calculate total kWh and carbon intensity
    energy_sources = [col for col in generation_df.columns if col not in ['Date', 'Time', 'MTU']]
    generation_df['Total_kWh'] = generation_df[energy_sources].sum(axis=1)
    generation_df['Carbon_Intensity'] = generation_df[energy_sources].apply(
        lambda row: sum(row[source] * energy_values.get(source, 0) for source in energy_sources) / generation_df['Total_kWh'],
        axis=1
    )
    print(generation_df.columns)
    print(generation_df['Total_kWh'])
    print(generation_df['Carbon_Intensity']) 
    assert not np.isnan(generation_df['Carbon_Intensity'].values[0])
    return generation_df['Carbon_Intensity'].values[0]
