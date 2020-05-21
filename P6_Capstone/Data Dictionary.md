|FINAL DATA MODEL|
|-----|

| Column | Type | Source | Description |
|---|---|---|---|
id | integer | Immigration Data | Unique ID for immigration data. Represents a traveler who is coming to US
Month | integer | Immigration Data | Month of the dataset
Year | integer | Immigration Data | Year of the dataset
Arrival_Date | date | Immigration Data | Date traveler arrived in US
Departure_Date | date | Immigration Data | Date traveler left in US
Trip_Duration | integer | Immigration Data | Trip of duration in days. <br><br>Derived using arrival_date and departure_date
Visa_Type | integer | Immigration Data | Type of visa for the traveler. <br><br>1: Business<br> 2: Pleasure<br> 3: Student
Gender | string | Immigration Data | Gender of traveler
Age | integer | Immigration Data | Age of traveler
US_State_Name | string | Immigration Data | Full state name where the person arrives
State_Temp | double | State Temperature Data | Average temperature (in Celcius) for the state for the month of April from the year 2000-2013 <br><br> Used this timeframe since the immigation data only contains data from April 2016.
State_Population | long | Demographics Data | Total population of the state traveler arrives in
State_Male_Population | double | Demographics Data | Percent of population that are male for the state the traveler arrives in
State_Female_Population | double | Demographics Data | Percent of population that are female for the state the traveler arrives in
Origin_Country | string | Immigration Data | Full name of country the traveler is from
Origin_Country_Temp | double | Country Temperature Data | Average temperature (in Celcius) for the state for the month of April from the year 2000-2013 <br><br> Used this timeframe since the immigation data only contains data from April 2016.
Airport_Name | string | Airport Data | Name of the airport the traveler arrives in
Airport_State | string | Airport Data | State that the airport is located
Airline | string | Airport Data | Airline code for the airline the traveler arrived on
Mode_Transportation | string | Immigration Data | Method of transportation used for travel. <br><br>1: Air <br> 2: Sea<br> 3: Land<br> 9: Not reported

<br><br>

|IMMIGRATION DATA|
|-----|

| Column | Type | Description |
|---|---|---|
id | integer | Numeric ID given to each row
month | integer | Month of the dataset
year | integer | Year of the dataset
arrival_date | date | Arrival date
departure_date | date | Departure date
trip_duration | integer | Trip of duration in days. <br><br>Derived using arrival_date and departure_date
origin_country_code | string | Two character country code where the person is from (Example: US = United States)
origin_country | string | Full country name where the person is from
transportation_mode | integer | Method of transportation used for travel. <br><br>1: Air, 2: Sea, 3: Land, 9: Not reported
us_state | string | Two character state code where the person traveled to <br><br>(Example: HI = Hawaii)
us_state_name | string | Full state name where the person traveled to
port | string | Port code that traveler entered the US through
gender | string | Gender of traveler
age | integer | Age of traveler
airline | string | Airline code for traveler
visa_type | integer | Type of visa for the traveler. <br><br>1: Business; 2: Pleasure; 3: Student

<br><br>

|AIRPORT DATA|
|-----|

| Column | Type | Description |
|---|---|---|
iata_code | string | Unique airport code used to identify airports
name | string | Name of airport
type | string | Type of airport
elevation_ft | integer | Elevation of airport
us_state | string | US state where airport is located. Two character code that identifies state.

<br><br>

|DEMOGRAPHICS DATA|
|-----|

| Column | Type | Description |
|---|---|---|
State | string | Name of state
StateCode | string | State code. Two character code that identifies state
MedianAge | double | Median age of state
MalePopulation | long | Total population of males
FemalePopulation | long | Total population of males
TotalPopulation | long | Total population of state
NumberVeterans | long | Count of veterans in the state
Foreign-born | long | Count of foreign born people in the state
AvgHouseholdSize | double | Average household size
AmericanIndianAlaskaNativeTotal | long  | Total number of American Indians and Alaska Natives
AsianTotal | long  | Total number of Asian people in the state
BlackTotal | long  | Total number of Black people in the state
HispanicTotal | long  | Total Number of Hispanic people in the state
WhiteTotal | long  | Total Number of White people in the state
MalePopPercent | double | Percent (as a decimal) of males in the state <br><br>Calculated using MalePopulation / TotalPopulation
FemalePopPercent | double | Percent (as a decimal) of females in the state <br><br>Calculated using FemalePopulation / TotalPopulation
VeteranPopPercent | double | Percent (as a decimal) of veterans in the state <br><br>Calculated using NumberVeterans / TotalPopulation
ForeignPopPercent | double | Percent (as a decimal) of foreign born people in the state <br><br>Calculated using Foreign-born / TotalPopulation
Black_pct | double | Percent (as a decimal) of Black people in the state <br><br>Calculated using BlackTotal / TotalPopulation
White_pct | double | Percent (as a decimal) of White people in the state <br><br>Calculated using WhiteTotal / TotalPopulation
Asian_pct | double | Percent (as a decimal) of Asian people in the state <br><br>Calculated using AsianTotal / TotalPopulation
Hispanic_pct | double | Percent (as a decimal) of Hispanic in the state <br><br>Calculated using HispanicTotal / TotalPopulation
AmericanIndianAlaskaNative_pct | double | Percent (as a decimal) of American Indian and Alaska Natives in the state <br><br>Calculated using AmericanIndianAlaskaNativeTotal / TotalPopulation

<br><br> 

|STATE TEMPERATURE DATA|
|-----| 

| Column | Type | Description |
|---|---|---|
|State | string | Full name of state
state_code | string | Two character code that identifies state
AvgTemp | double | Average temperature for the state for the month of April from the year 2000-2013 <br><br> Used this timeframe since the immigation data only contains data from April 2016.

<br><br>

|COUNTRY TEMPERATURE DATA|
|-----|

| Column | Type | Description |
|---|---|---|
Country | string | Full name of country 
country_code | string | Two character code that identifies the country
AvgTemp | double | Average temperature for the country for the month of April from the year 2000-2013 <br><br> Used this timeframe since the immigation data only contains data from April 2016.