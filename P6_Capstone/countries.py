from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
#may need to install pycountry first
import pycountry

countries = {}
# Get a dictionary of key value pairs. Key is country name and the value is a 2 digit 
# string that is a standardized code. 
for country in pycountry.countries:
    countries[country.name] = country.alpha_2

# A country name may be used differently in the data you are working with
# The extra_codes dictionary allows for additional country names
# That did not result in any country codes.
# This can later be recustomized for any country names that return a null when using pycountry
extra_codes = {'Bolivia' : 'BO',
 'Bosnia And Herzegovina' : 'BA',
 'Burma' : 'MM',
 'Congo (Democratic Republic Of The)' : 'CD',
 'Czech Republic' : 'CZ',
 "Côte D'Ivoire" : 'CI',
 'Guinea Bissau' : 'GW',
 'Iran' : 'IR',
 'Laos' : 'LA',
 'Macedonia' : 'MK',
 'Moldova' : 'MD',
 'Reunion' : 'RE',
 'Russia' : 'RU',
 'South Korea' : 'KR',
 'Swaziland' : 'SZ',
 'Syria' : 'SY',
 'Taiwan' : 'TW',
 'Tanzania' : 'TZ',
 'Venezuela' : 'VE',
 'Vietnam' : 'VN',
 'CHINA, PRC': 'CN',
 'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)': 'MX',
 'SOUTH KOREA': 'KR',
 'ARGENTINA ': 'AR',
 'VENEZUELA': 'VE',
 'TAIWAN': 'TW',
 'RUSSIA': 'RU',
 'CZECH REPUBLIC': 'CZ',
 'VIETNAM': 'VN',
 'BOLIVIA': 'BO',
 'BRITISH VIRGIN ISLANDS': 'VG',
 'IRAN': 'IR',
 'ANTIGUA-BARBUDA': 'AG',
 'ST. LUCIA': 'LC',
 'BURMA': 'MM',
 'ST. KITTS-NEVIS': 'KN',
 'MOLDOVA': 'MD',
 'MACEDONIA': 'MK',
 'PALESTINE': 'PS',
 'BOSNIA-HERZEGOVINA': 'BA',
 'CURACAO': 'CW',
 'SYRIA': 'SY',
 'IVORY COAST': 'CI',
 'ST. VINCENT-GRENADINES': 'VC',
 'CAPE VERDE': 'CV',
 'KAMPUCHEA': 'KH',
 'TANZANIA': 'TZ',
 'ZAIRE': 'AO',
 'KOSOVO': 'RS',
 'REUNION': 'RE',
 'NETHERLANDS ANTILLES': 'AN',
 'MACAU': 'MO',
 'SAINT MAARTEN': 'MF',
 'SWAZILAND': 'SZ',
 'LAOS': 'LA',
 'BRUNEI': 'BN',
 'MICRONESIA, FED. STATES OF': 'FM',
 'NORTH KOREA': 'KP',
 'ST. PIERRE AND MIQUELON': 'PM',
 'FAROE ISLANDS (PART OF DENMARK)': 'FO',
 'REPUBLIC OF SOUTH SUDAN': 'SS',
 'BONAIRE, ST EUSTATIUS, SABA': 'BQ',
 'SAINT MARTIN': 'MF',
 'HOLY SEE/VATICAN': 'VA',
 'WALLIS AND FUTUNA ISLANDS': 'WF',
 'FALKLAND ISLANDS': 'FK',
 'PITCAIRN ISLANDS': 'PN',
 'EAST TIMOR': 'TL',
 'YUGOSLAVIA': 'YU',
 'COCOS ISLANDS': 'CC',
 'MAYOTTE (AFRICA - FRENCH)': 'YT',
 'SAINT BARTHELEMY': 'BL',
 'ST. HELENA': 'SH',
 'Timor Leste' : 'TL',
 'Baker Island' : 'UM',
 'Kingman Reef' : 'UM',
 'Guinea Bissau' : 'GW'
}

# Merge pycountry dictionary and custom dictionary
countries.update(extra_codes)

# Make all keys uppercase
countries = {k.upper(): v for k,v in countries.items()}

"""
This function uses the dictionary above to return a standardized country code
when taking in a country name.

Part of this dictionary is from the pycountry package. However, sometimes country names
are not uniformly captured. I added additional key value pairs that have country names and codes that  
were not captured in the pycountry package.
"""

get_country_code = udf(lambda x: countries.get(x, 99999), StringType())