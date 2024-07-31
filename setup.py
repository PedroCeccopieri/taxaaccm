# requirements
# ! pip install requests
# ! pip install zipfile36
# ! pip install icalendar
# ! pip install timezonefinder

import pandas as pd
import numpy as np
import math as mt
import requests as rq
import zipfile as zf
import shutil as sh
import os

from icalendar import Calendar, Event
import datetime
import timezonefinder
import pytz

from functools import reduce

pd.options.mode.copy_on_write = True

fpath = os.path.dirname(__file__) + "\\WCA_database"

if (__name__ == "__main__"):

  # Creating folder
  try:  
    os.mkdir(fpath)  
  except OSError as error:  
      print(f"{fpath} already exist")

  # Download database
  r = rq.get('https://www.worldcubeassociation.org/export/results/WCA_export.tsv.zip')

  with open(fpath + '/wcaDatabase.zip', 'wb') as f:
    f.write(r.content)

  # Unzip database
  with zf.ZipFile(fpath + '/wcaDatabase.zip') as f:
    f.extractall(path = fpath)

  # Deleting .zip
  os.remove(fpath + '/wcaDatabase.zip')
  os.remove(fpath + '/metadata.json')
  os.remove(fpath + '/README.md')

# Importing tables
competitions = pd.read_csv(fpath + '/WCA_export_Competitions.tsv', sep='\t')
events = pd.read_csv(fpath + '/WCA_export_Events.tsv', sep='\t')
results = pd.read_csv(fpath + '/WCA_export_Results.tsv', sep='\t')
roundTypes = pd.read_csv(fpath + '/WCA_export_RoundTypes.tsv', sep='\t')

eventsNames = list(events["cellName"])

taxPerPerson = 1.5

def calculate(compId):

  # Download Schedule
  r = rq.get(f'https://www.worldcubeassociation.org/competitions/{compId}.ics')

  with open(fpath + f'/{compId}.ics', 'wb') as f:
    f.write(r.content)

  days = []
  sche = []

  g = open(fpath + f'/{compId}.ics','rb')
  gcal = Calendar.from_ical(g.read())

  # Get timezone
  tf = timezonefinder.TimezoneFinder()
  la = competitions.loc[competitions["id"] == compId]["latitude"].iloc[0]/1000000
  lo = competitions.loc[competitions["id"] == compId]["longitude"].iloc[0]/1000000
  timezone_str = tf.certain_timezone_at(lat = la, lng = lo)

  for component in gcal.walk():

    if (component.name == "VEVENT"):

      summary = component.get('summary') # Event name
      date = component.get('dtstart').dt # Event date UTC 0

      if (timezone_str is None):
          return -1
      else:
          timezone = pytz.timezone(timezone_str)
          date = date.astimezone(timezone).date() # Event date local UTC

      # Adding days
      if (date not in days):
        days.append(date)
        sche.append(set([]))

      for evt in eventsNames:
        if (evt in summary):

          aux = [evt]

          if ("First" in summary):
            aux.append("First round")
          elif ("Second" in summary):
            aux.append("Second round")
          elif ("Semi" in summary):
            aux.append("Semi Final")
          elif ("Final" in summary):
            aux.append("Final")

          if ("(Attempt 1)" in summary):
            aux.append("a1")
          elif ("(Attempt 2)" in summary):
            aux.append("a2")
          elif ("(Attempt 3)" in summary):
            aux.append("a3")
          else:
            aux.append("a0")

          if (len(aux) == 3):
            sche[days.index(date)].add(tuple(aux))

  g.close()

  # Delete the schedule file
  os.remove(fpath + f'/{compId}.ics')

  if (len(sche) == 0):
    return -2

  # Get competitors participations
  competitors = results.loc[results["competitionId"] == compId, ["personId", "eventId", "roundTypeId", "value1", "value2", "value3"]]
  competitors = competitors.merge(events[["id", "name"]].rename(columns={"id":"eventId", "name": "event"}))
  competitors = competitors.merge(roundTypes[["id", "name"]].rename(columns={"id":"roundTypeId", "name": "roundName"}))
  competitors = competitors[["personId", "event", "roundName", "value1", "value2", "value3"]]
  competitors = competitors.itertuples(index=False, name=None)

  personsPerDay = [[] for i in sche]
  schedule = {f"{e} {r} {a}": idx for idx, i in enumerate(sche) for e, r, a in i}

  # Parse every person on each day competed
  for cId, evt, rd, v1, v2, v3 in competitors:

    # In case there is something wrong with the schedule in WCA
    try:

      if (evt == "3x3x3 Multi-Blind" or evt == "3x3x3 Fewest Moves"):
        if (v1 != 0 and v1 != -2):
          personsPerDay[schedule[f"{evt} {rd} a1"]].append(cId)
        if (v2 != 0 and v2 != -2):
          personsPerDay[schedule[f"{evt} {rd} a2"]].append(cId)
        if (v3 != 0 and v3 != -2):
          personsPerDay[schedule[f"{evt} {rd} a3"]].append(cId)
      else:
        personsPerDay[schedule[f"{evt} {rd} a0"]].append(cId)

    except:

      return -3

  personsPerDay = [set(i) for i in personsPerDay]

  value = sum([len(i) * taxPerPerson for i in personsPerDay])

  return value

def f(row):
  return ((datetime.datetime(row["year"], row["endMonth"], row["endDay"]) - datetime.datetime(row["year"], row["month"], row["day"])).days + 366) % 365

def simulation(diasComp, qntCompetidores):

  c = results[["competitionId", "personId"]]
  c = c.drop_duplicates()
  c = c[["competitionId"]]
  c["competitors"] = np.nan
  c = c.groupby('competitionId').size().reset_index().rename(columns={0:"competitors"})

  d = competitions[["id","year","endMonth","endDay","month","day"]]
  d["days"] = d.apply(f, axis=1)
  d = d[["id","days"]].rename(columns={"id":"competitionId"})

  com = c.merge(d)

  a = com.loc[com["days"] == diasComp]

  if (a.empty):
    print("Não existem competições com essa quantidade de dias")
    return -1

  a = a.loc[com["competitors"] == qntCompetidores]
  c = 1

  while (a.empty):

    if (qntCompetidores - c <= 0):
      print("não foi possivel encontrar competições semelhantes")
      return -1

    b1 = com.loc[com["days"] == diasComp].loc[com["competitors"] == qntCompetidores + c]
    b2 = com.loc[com["days"] == diasComp].loc[com["competitors"] == qntCompetidores - c]
    a = pd.concat([b1,b2])
    c += 1

  txs = [calculate(row["competitionId"]) for idx, row in a.iterrows()]
  txs = [i for i in txs if (i > 0)]
  print(f"competições semelhantes: {len(txs)}")
  print(f"diferença de competidores: {c}")
  print(sum(txs)/len(txs))
  return 1