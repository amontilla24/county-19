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

# In[203]:


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


# In[204]:


# Filter data by a given county
#
def filter_by_county(county):
    filtered = us_counties[(us_counties['county']==county)]
    filtered = filtered.drop({'county', 'state', 'fips'},  axis=1).reset_index(drop=True)
    return filtered


# In[205]:


# Filter data by a given state
#
def filter_by_state(state):
    return us_states[(us_states['state']==state)].drop({'state', 'fips'},  axis=1).reset_index(drop=True)


# In[206]:


# Filter US data
#
def filter_us():
    return us.reset_index(drop=True)


# In[207]:


# Upload information info (county and state list) to firebase
#
def upload_info():
    start = time.time()
    firebase_db.put(url='/info/', data=str(states.tolist()).strip('[]').replace('\'','').replace(', ', ','), name='states')
    firebase_db.put(url='/info/', data=str(counties_info.tolist()).strip('[]').replace('\'','').replace(', ', ','), name='counties')
    end = time.time()
    logging.info('\tUploaded info data (' + str(format(end-start, '.3f')) + ' secs)')


# In[208]:


# Upload given data to specific dir
#
def firebase_upload(dir, data, name):
    # TODO: might want to send more data in the future, for now this will do
    firebase_db.put(url=dir, data=data.head(360).to_dict('records'), name=name)


# In[209]:


def upload_us():
    start = time.time()
    firebase_upload(dir='/', data=filter_us(), name='us')
    end = time.time()
    logging.info('\tUploaded US data (' + str(format(end-start, '.3f')) + ' secs)')


# In[210]:


def upload_states():
    start = time.time()
    for state in states:
        firebase_upload(dir='/state/', data=filter_by_state(state), name=state)
    end = time.time()
    logging.info('\tUploaded states data (' + str(format(end-start, '.3f')) + ' secs)')


# In[211]:


def upload_counties(n):
    start = time.time()
    for county in counties[n%len(counties)]:
        firebase_upload(dir='/county/', data=filter_by_county(county), name=county.replace(".", ""))
    end = time.time()
    logging.info('\tUploaded county data (' + str(format(end-start, '.3f')) + ' secs)')


# In[212]:


def upload_all_data(n):
    start = time.time()
    upload_info()
    upload_us()
    upload_states()
    upload_counties(n)
    end = time.time()
    logging.info('  Total upload time: (' + str(format(end-start, '.3f')) + ' secs)')


# In[213]:


def partition(county_list, n):
    for i in range(0, len(county_list), n):
        yield county_list[i:i + n]


# In[214]:


# Init log
logging.basicConfig(filename='download_history.log',level=logging.INFO)

# Refresh files through git
clone_dir = './'
repo_dir = './covid-19-data'
repo_url = 'https://github.com/nytimes/covid-19-data.git'

if not os.path.isdir('./covid-19-data'):
    git.Git(clone_dir).clone(repo_url)
    logging.info(str('\tCloned repo: ' + repo_url))
        
global git_repo
global us_counties
global us_states
global us
global firebase_url
global firebase_db
global counties_info
global counties
global states

# Connect to firebase database
firebase_url = 'https://county-live-19.firebaseio.com/'
firebase_db = firebase.FirebaseApplication(firebase_url, None)


# In[ ]:


# Upload data to firebase
i = 0
while True:
    logging.info('\t========== ['+datetime.today().strftime('%m/%d/%Y %H:%M:%S')+'] =========')
    git_repo = git.cmd.Git(repo_dir)
    logging.info(str("\tPulled data: "+git_repo.pull()))
    
    # Populate dataframes
    start = time.time()
    us_counties = pd.read_csv('covid-19-data/us-counties.csv')
    us_states = pd.read_csv('covid-19-data/us-states.csv')
    us = pd.read_csv('covid-19-data/us.csv')
    us_counties.sort_values(by=['date'], inplace=True, ascending=False)
    us_states.sort_values(by=['date'], inplace=True, ascending=False)
    us.sort_values(by=['date'], inplace=True, ascending=False)
    end = time.time()
    logging.info(str('\tPopulated dataframes (' + str(format(end-start, '.3f')) + ' secs)'))
    counties_info = us_counties.county.unique()
    counties = list(partition(counties_info, 60))
    states = us_states.state.unique()

    # Upload data
    upload_all_data(i)
    i = (i + 1) % len(counties)
    time.sleep(1200)


# #### Notes
# Command to convert notebook to python:
# jupyter nbconvert --to python County_19_Notebook.ipynb
