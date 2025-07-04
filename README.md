# CO2eq Publisher

This project provides a service to calculate and publish the carbon intensity (CO2 equivalent) of electricity generation in Northern Italy using ENTSO-E data. The carbon intensity is published via MQTT for integration with other systems.

## Features
- Fetches real-time electricity generation data from ENTSO-E.
- Calculates carbon intensity (gCO2eq/kWh) based on energy source mix.
- Publishes the calculated value to an MQTT broker at regular intervals.
- Designed to run as a systemd service.

## Requirements
- Python 3.8+
- MQTT broker (e.g., Mosquitto)
- ENTSO-E API key (see below)

### Python Dependencies
Install dependencies using pip:

```bash
pip install -r requirements.txt
```

## Setup

1. **ENTSO-E API Key**
   - Obtain an API key from [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).
   - Create a file named `entsoe_key.json` in the project directory with the following content:
     ```json
     { "ENTSOE_KEY": "YOUR_API_KEY_HERE" }
     ```

2. **MQTT Broker**
   - Ensure an MQTT broker is running and accessible.
   - You can change the broker address in `co2_publisher.py` if needed.

3. **Run the Publisher**
   - To run manually:
     ```bash
     python co2_publisher.py
     ```
   - The script will periodically publish the carbon intensity value to the MQTT topic:
     ```
     org/unibo/cluster/hifive/node/<hostname>/plugin/coe_calulator/chnl/data/carbon_intensity
     ```

4. **Systemd Service (Optional)**
   - To run as a service, use the provided `co2eq.service` file.
   - Edit the paths in `co2eq.service` to match your environment if necessary.
   - Copy the service file to `/etc/systemd/system/`:
     ```bash
     sudo cp co2eq.service /etc/systemd/system/
     sudo systemctl daemon-reload
     sudo systemctl enable co2eq.service
     sudo systemctl start co2eq.service
     ```

## Files
- `co2_publisher.py`: Main script to calculate and publish carbon intensity.
- `coe_calculator.py` / `GPT_coe_calculator.py`: Functions to fetch and compute carbon intensity from ENTSO-E data.
- `requirements.txt`: Python dependencies.
- `co2eq.service`: Example systemd service file.

## Notes
- The script is configured for the IT_NORD (Northern Italy) ENTSO-E region. You can modify the country code in the scripts for other regions.
- Make sure the Python interpreter path in the shebang (`#!/home/examon/.venv/bin/python`) matches your environment, or run with your active Python environment.

## License
MIT License
