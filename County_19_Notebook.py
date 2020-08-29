#!/usr/bin/env python
# coding: utf-8

# # County 19

# ## Project Introduction
# ### Functional Distribution
# #### Pyton Scripts
# Scrapes data from New York Times GitHub project
# 
# #### Firebase
# Keeps the clean data and stores it based on county and time
# 
# #### iOS / Android App (Flutter?)
# Frontend to access the Data

# In[45]:


# Imports
import sys
import numpy as np
import pandas as pd
import git
from datetime import datetime, timedelta
from firebase import firebase
import time
import json
import os
import logging


# In[46]:


# Filter data by a given county
#
def filter_by_county(county):
    filtered = us_counties[(us_counties['county']==county)]
    filtered = filtered.drop({'county', 'state', 'fips'},  axis=1).reset_index(drop=True)
    return filtered


# In[47]:


# Filter data by a given state
#
def filter_by_state(state):
    return us_states[(us_states['state']==state)].drop({'state', 'fips'},  axis=1).reset_index(drop=True)


# In[48]:


# Filter US data
#
def filter_us():
    return us.reset_index(drop=True)


# In[49]:


# Upload information info (county and state list) to firebase
#
def upload_info():
    print('Uploading info data...')
    start = time.time()
    firebase_db.put(url='/info/', data=json.dumps(states.tolist()), name='states')
    firebase_db.put(url='/info/', data=json.dumps(counties_info.tolist()), name='counties')
    end = time.time()
    print('Finished uploading info data (' + str(format(end-start, '.3f')) + ' secs)')


# In[50]:


# Upload given data to specific dir
#
def firebase_upload(dir, data):
    # TODO: might want to send more data in the future, for now this will do
    firebase_db.put(url=dir, data=data.head(90).to_json(orient ='index'), name='quarter')


# In[51]:


def upload_us():
    print('Uploading US data...')
    start = time.time()
    firebase_upload('/us/', filter_us())
    end = time.time()
    print('Finished uploading US data (' + str(format(end-start, '.3f')) + ' secs)')


# In[52]:


def upload_states():
    print('Uploading state data...')
    start = time.time()
    for state in states:
        firebase_upload('/state/'+state+'/', filter_by_state(state))
    end = time.time()
    print('Finished uploading states data (' + str(format(end-start, '.3f')) + ' secs)')


# In[53]:


def upload_counties(n):
    print('Uploading county data...')
    start = time.time()
    for county in counties[n%len(counties)]:
        firebase_upload('/county/'+county.replace(".", "")+'/', filter_by_county(county))
    end = time.time()
    print('Finished uploading county data (' + str(format(end-start, '.3f')) + ' secs)')


# In[54]:


def upload_all_data(n):
    start = time.time()
    upload_info()
    upload_us()
    upload_states()
    upload_counties(n)
    end = time.time()
    print('Total upload time: (' + str(format(end-start, '.3f')) + ' secs)')


# In[55]:


def partition(county_list, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(county_list), n):
        yield county_list[i:i + n]


# In[ ]:


# Init log
logging.basicConfig(filename='download_history.log',level=logging.INFO)

# Refresh files through git
clone_dir = './'
repo_dir = './covid-19-data'
repo_url = 'https://github.com/nytimes/covid-19-data.git'

if not os.path.isdir('./covid-19-data'):
    git.Git(clone_dir).clone(repo_url)
    logging.info(str('Pulled data from ' + repo_url))
        
git_repo = git.cmd.Git(repo_dir)
logging.info(str("Pulling data:"+git_repo.pull()))

# Populating dataframes
logging.info('Populating dataframes... ')
start = time.time()
us_counties = pd.read_csv('covid-19-data/us-counties.csv')
us_states = pd.read_csv('covid-19-data/us-states.csv')
us = pd.read_csv('covid-19-data/us.csv')
us_counties.sort_values(by=['date'], inplace=True, ascending=False)
us_states.sort_values(by=['date'], inplace=True, ascending=False)
us.sort_values(by=['date'], inplace=True, ascending=False)
end = time.time()
logging.info(str('Success populating dataframes (' + str(format(end-start, '.3f')) + ' secs)'))

# Connect to firebase database
firebase_url = 'https://county-19.firebaseio.com/'
firebase_db = firebase.FirebaseApplication(firebase_url, None)

# Get information data
counties_info = us_counties.county.unique()
counties = list(partition(counties_info, 60))
states = us_states.state.unique()

# Upload data to firebase
i = 0
while True:
    logging.info(str("Pulling data:"+git_repo.pull()))
    logging.info(str('Uploading Data ['+datetime.today().strftime('%m/%d/%Y %H:%M:%S')+']'))
    upload_all_data(i)
    i = (i + 1) % len(counties)
    time.sleep(1200)


# #### Notes
# Command to convert notebook to python: 
# jupyter nbconvert --to python County_19_Notebook.ipynb
