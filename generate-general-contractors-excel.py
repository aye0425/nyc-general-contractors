"""
Author: Andy Ye
Created: 10th November, 2022
"""

# Imports
import os
import time
import requests as req
import pandas as pd
import numpy as np
import logging
import glob as gl
from bs4 import BeautifulSoup
from os import chdir
from pathlib import Path
from requests.adapters import HTTPAdapter
from openpyxl import Workbook, load_workbook

chdir(Path(__file__).parent.absolute())

# Globals
wb_file = 'active-general-contractors.xlsx'

url = 'https://a810-bisweb.nyc.gov/bisweb/ResultsByNameServlet'

contractor_url = 'https://a810-bisweb.nyc.gov/bisweb/LicenseQueryServlet'

columns =['LICENSEE', 'STATUS', 'BUSINESS', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE', 'PHONE']

# Request Parameters
cookies = {
    'JSESSIONID': '3D4E31E56B378EB6D36518F28A26FDDC',
    '__utmz': '24711658.1667776402.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
    '_ga': 'GA1.2.1797655611.1668129953',
    'nmstat': 'a10e0ec0-0976-9fb6-cb57-9fcc420ed2b1',
    'WT_FPC': 'id=1d1ee37e-7570-4084-9364-cbb8660154d8:lv=1668130337302:ss=1668130284667',
    'bm_sz': '977E0FF123FD98BA0D712906622373CF~YAAQzjoiF3VK4mCEAQAAS1lMbhEcsLhhCipsnG6beMxEeCqFZFGJ0q42RRf/f2q+SynB8P63ExHj7A7GTO82vBI5FrjHkQegtzm5SgUaffg0HOlJE5jfNJEF16Ii4HmLvpRdF2Y1F324Xq+Ho87wxlpHHoLkuG2CeZo4lA0ku3HntOVdX+TTg2V1Lna2QMDurKWDr3nS0ivPCD+qMnaCSi95PZ5SD0u34ysb170EZtniTVgKyjg5P/HE/5y0jhc2e/npLl9KmhXfHtg4nNmb6wGPbg11ClsnqxenEzjanp8=~3553585~4405058',
    'RT': '"z=1&dm=nyc.gov&si=616f9708-68d1-42ee-862f-9034840dc5f0&ss=laelfv73&sl=0&tt=0"',
    '_gid': 'GA1.2.598991610.1668297809',
    'ak_bmsc': '278DF7E6BD04B6CBE943B4F539F77156~000000000000000000000000000000~YAAQzjoiF1pL4mCEAQAAY15MbhHEleblS32QTb2/M4j6DHR1qlqnIWBYqVyIrPBgcQ6M4HeaAiwsYVAmi1ctFPzYTAU4wNNifRfvU0vjtP3IS/p/BXczuq1Z1bt8Uc03Qpz2d4kkooU+xkH9EMyETf8vJMXE1rmIUvN+Jf3fuLPMPPCau7jFNu0VrxuF+uf7TMf8vgifl8NOJTFwgQXH1kGQL9x6godqMCS6H8jZ9oKH6Lg4mWDnuJqMM7e+v5OLV6bSf2ATZOLPZNNYOMt00EYJ/1fbiq9pyFIBByR5gJpZp7HS3UTayTnk/5ZPFnDDhzooXnXurPe0nDHaJqJ6Onetvi8sVcI3zRnKen8Ocxhi/hOt4z9cp6NDhwFN0Td/kG1pCoDA+Su17+An2iYnZOUty4og7gLU9cG/tznx3/Bqgb0A6uA5dg+6WHqL91W2vtgfi15wh06/IHkqsAiaPY8V5DZrzYHCXB7I4HK/sNCoBdd6yw==',
    '__utma': '24711658.614898629.1667776402.1668225620.1668297964.8',
    '__utmc': '24711658',
    '__utmt': '1',
    'WT_FPC': 'id=1d1ee37e-7570-4084-9364-cbb8660154d8:lv=1668297963958:ss=1668297963958',
    'akavpau_wr': '1668298664~id=a28cac98d867f283e6dc144c467f3501',
    '_abck': '6FE16E468558C714B987BC112473B573~0~YAAQEBAoFzCl5jyEAQAAB9hUbgipSvICDdwNwRiTJaH7vcXvoDe1b1ahI3Rx9/vhXXoWUuTkSmVvrOSWasl6+1hr7RWYXzS0gQ+HahjB5LYb3LZNbiH8HvmBjO5pCsRYVDm/Zb+GOUXrk2qQGD9OE7qANXUb4b1tEp4l2ODyNxi2cjImasl0swoZ7e6iKd00CgKX9/XT9WK4iIKAmBFcWlzlul9YimqET7Npdx4pwbYfcu9m8sDvRphEVL7iE28UgltYMvsh/I8Yp2Wd7NVT246jd6+KLp/mEh4tKmZa/Ewpj3QFaBCy9RRDQr+NqFsyt9iHjun94Vjdc7jLoof3fR4Rw/PKh2YOpScuN+Y8gkqvELZ+gy/VEqhnDA==~-1~-1~-1',
    'bm_sv': 'F3D2FC52BF71E08873564CB7CBEAC2EF~YAAQEBAoFzGl5jyEAQAAB9hUbhGmbE9fiiVrdDh8n5eeA+4lxez3R7ihMRkVcixtFYjZyfu6S9ia5vTnbjYT+c7nlfpZc+Plv78XXooGWGAoYCt/zsSx/Wc3gjCHUH28SBj4FAPVbqrChZicNgs7c+Yz4vpQaK1zkiF8FHb1XrwF02/IkRBJNk0n5b96rncEA9oy77oFbPD5fKiLrvEj2ZVdd5eAo2wssB6pM1tLT5C8MNUPbGXbUecQeGZg~1',
    '__utmb': '24711658.7.10.1668297964',
}

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

requestId = 0

params = {
  "allcount": "0001",
  "licensetype": "G",
  "licname": "",
  "licstatus": "A",
  "requestId": requestId
}

contractor_params = {
  "licensetype": "G",
  "licno": "",
  "requestId": requestId + 1
}

# Logger
logging.basicConfig(filename='script.log', format='%(asctime)s %(levelname)s:%(message)s', filemode='w')

logger = logging.getLogger()

logger.setLevel(logging.INFO)

# Modules
def consolidate_csv():
  filenames = sorted([i for i in gl.glob('./cvs-results/*.csv')])
  print(filenames)

  writer = pd.ExcelWriter(wb_file, engine='xlsxwriter')
  
  for csv_file in filenames:
    col_names = ['LICENSEE', 'STATUS', 'BUSINESS', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE', 'PHONE']
    df = pd.read_csv(csv_file)
    df.to_excel(writer, columns=col_names, sheet_name=csv_file[14])
  
  writer.close()

def split_address(contract_info, address):
  addr_list = address.split('   ');

  if len(addr_list) == 2:
    addr = addr_list[0].strip().replace(',', '')
    city = addr_list[1].split(',')[0]

    if(len(addr_list[1].split(','))) < 3:
      state = addr_list[1].split(',')[1].strip().split()[0]
      zipcode = addr_list[1].split(',')[1].strip().split()[1]
    else:
      state = addr_list[1].split(',')[2].strip().split()[0]
      zipcode = addr_list[1].split(',')[2].strip().split()[1]
  elif len(addr_list) == 3:
    addr = (addr_list[0].strip() + " " + addr_list[1].strip()).replace(',', '')
    city = addr_list[2].split(',')[0]

    if(len(addr_list[2].split(','))) < 3:
      state = addr_list[2].split(',')[1].strip().split()[0]
      zipcode = addr_list[2].split(',')[1].strip().split()[1]
    else:
      state = addr_list[2].split(',')[2].strip().split()[0]
      zipcode = addr_list[2].split(',')[2].strip().split()[1] 

  contract_info.append(addr.strip())
  contract_info.append(city)
  contract_info.append(state)
  contract_info.append(zipcode)

def parse_contractor_information(contract_id, session):
  success = False
  
  while not success:
    try: 
      contractor_params['licno'] = contract_id

      resp = session.get(contractor_url, headers=headers, params=contractor_params, cookies=cookies)
      logging.info(resp.url)

      soup = BeautifulSoup(resp.content, 'html.parser')

      table = soup.findAll('table')[3].find_all('tr')

      contractor_info = []
      
      if len(table[4].text.strip()[16:]) <= 3:
        split_address(contractor_info, table[5].text.strip()[16:])
        contractor_info.append(table[6].text.strip()[17:])
      else:
        split_address(contractor_info, table[4].text.strip()[16:])
        contractor_info.append(table[5].text.strip()[17:])

      success = True

      return contractor_info

    except req.exceptions.RequestException as e:
      logging.exception(e)
      time.sleep(60)
    except IndexError as e:
      logging.exception(e)
      time.sleep(60)

def main():
  s = req.Session()

  for i in range(26):
    params['licname'] = chr(i + 65)
    licensees = []
    table_length = 71

    while table_length >= 71:
      success = False
      
      while not success:
        try:
          resp = s.get(url, headers=headers, params=params, cookies=cookies)
          logging.info(resp.url)

          soup = BeautifulSoup(resp.content, 'html.parser')

          table = soup.findAll('table')[3]

          success = True
        except req.exceptions.RequestException as e:
          logging.exception(e)
          time.sleep(60)
        except IndexError as e:
          logging.exception(e)
          time.sleep(60)

      table_length = len(table.find_all('tr'))
      logging.debug(table_length)

      allcount = int(params['allcount']) + 70
      params["allcount"] = "{:04d}".format(allcount)

      index = 0

      for row in table.find_all('tr'):
        cols = row.find_all('td')

        if len(cols) == 7 and index != 0:
          contractor_id = cols[1].text.strip()
          contractor_id = contractor_id[0:6]
          contractor_info = parse_contractor_information(contractor_id, s)

          licensees.append((
            cols[0].text.strip(),
            cols[2].text.strip(),
            cols[4].text.strip(),
            contractor_info[0],
            contractor_info[1],
            contractor_info[2],
            contractor_info[3],
            contractor_info[4]))
        
        index += 1

    licensee_array = np.asarray(licensees)
      
    df = pd.DataFrame(licensee_array, columns=columns)
    
    Path('/cvs-results').mkdir(parents=True, exist_ok=True)

    df.to_csv('./cvs-results/' + params['licname'] + '.csv', index=False)

    params['allcount'] = "0001"
  
  consolidate_csv()

# Run when file is directly executed
if __name__ == '__main__':
  main()