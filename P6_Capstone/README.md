# DATA ENGINEERING CAPSTONE

## Project Summary
This project will create a data model from various data sources. The final data model will allow analytics teams to gain insights into United States immigration data.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Conclusion and Notes

## Project Scope and Data
The objective of this project is to create a data pipeline that will combine datasets from multiple sources into a star schema data model. The data model will allow users to analyze immigration trends and other descriptive statistics. Analysts will be able to use infomration outside of the immigration dataset, such as data about temperature, US city demographics, and airport data to perform their analyses.

The data sources I am using can be found at the following links:
- [__I94 immigration data:__](https://travel.trade.gov/research/reports/i94/historical/2016.html) US immigration data from the US National Tourism and Trade Office 

- [__World temperature data:__](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data) Contains CSV files for overall global temperatures and temperatures by city, major city, state, country.

- [__US city demographics data:__](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/) Demographics data includes values such as population, race, age, and household size.

- [__Airport codes data:__](https://datahub.io/core/airport-codes#data) Contains data related to airports. Airports have three character codes that uniquely identify them. For example, LaGuardia airport's code is LGA.   

Using the final data model should allow analysts to answer questions such as:
- How many people immigrated from a certain country?
- Which US city had the most travelers in a given time period?
- How many people traveled to each state?
- How many immigrants came under each type of visa (student, business, pleasure)?

## Defining the Data Model

Using the four sources above, the data pipeline will create a star schema with fact and dimension tables. A star schema is a simple schema design that makes performing analytics queries very easy. An analytics team will benefit from using a star schema by allowing for simpler queries with easier to understand join logic from the business perspective. It will also boost query performance.

The final star schema data model will have the following fact and dimension tables (also available in data dictioanry with more detail):

__FACT TABLE__: 

- id, Month, Year,Arrival_Date, Departure_Date, Trip_Duration, Visa_Type, Gender, Age, <br>US_State_Name, State_Temp, State_Population, State_Male_Population, State_Female_Population, <br>Origin_Country, Origin_Country_Temp, Airport_Name, Airport_State, Airline, Mode_Transportation

<br>

__DIMENSION TABLES__:

__*Immigration Table*__:
- id, month, year, arrival_date, departure_date, trip_duration, origin_country_code, origin_country,<br> transportation_mode, us_state, us_state_name, port, gender, age, airline, visa_type

__*Airport Table*__:
- iata_code, name, type, elevation_ft, us_state

__*Demographics Table*__:
- State, StateCode, MedianAge, MalePopulation, FemalePopulation, TotalPopulation, NumberVeterans, <br>Foreign-born, AvgHouseholdSize, AmericanIndianAlaskaNativeTotal, AsianTotal, BlackTotal,<br> HispanicTotal, WhiteTotal, MalePopPercent, FemalePopPercent, VeteranPopPercent, TotalPopulation,<br> ForeignPopPercent, Black_pct, White_pct, Asian_pct, Hispanic_pct, AmericanIndianAlaskaNative_pct

__*State Temperature Table*__:
- State, state_code, AvgTemp 
__*Country Temperature Table*__:
- Country, country_code, AvgTemp
