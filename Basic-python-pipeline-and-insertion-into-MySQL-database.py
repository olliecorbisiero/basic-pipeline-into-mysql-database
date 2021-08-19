import re
import pandas as pd
import numpy as np
import random
from pandas.io.json import json_normalize
import requests
import pymysql
from sqlalchemy import create_engine

#list of schools to collect data on
schools = {
'ironhack' : 10828,
'app-academy' : 10525,
'springboard' : 11035,
'codingnomads': 10662,
'juno-college-of-technology': 10787,
'wild-code-school': 11169,
'udacity': 11118,
'the-tech-academy': 11091,
'neoland': 10906,
'flatiron-school': 10748,
}


def get_comments_school(school):
    TAG_RE = re.compile(r'<[^>]+>')
    # defines url to make api call to data -> dynamic with school if you want to scrape competition
    url = "https://www.switchup.org/chimera/v1/school-review-list?mainTemplate=school-review-list&path=%2Fbootcamps%2F" + school + "&isDataTarget=false&page=3&perPage=10000&simpleHtml=true&truncationLength=250"
    #makes get request and converts answer to json
    # url defines the page of all the information, request is made, and information is returned to data variable
    data = requests.get(url).json()
    #converts json to dataframe
    reviews =  pd.DataFrame(data['content']['reviews'])
  
    #aux function to apply regex and remove tags
    def remove_tags(x):
        return TAG_RE.sub('',x)
    reviews['review_body'] = reviews['body'].apply(remove_tags)
    reviews['school'] = school
    return reviews

#creating dataframe for comments
comments = []

for school in schools.keys():
    print(school)
    comments.append(get_comments_school(school))

comments = pd.concat(comments)
comments.drop(columns=['anonymous','hostProgramName', 'isAlumni', 'body', 'user', 'comments'], inplace=True)
comments[['overallScore','overall','curriculum','jobSupport']] = comments[['overallScore','overall','curriculum','jobSupport']].astype(float)

#function to generate dataframes for locations, courses, badges and school information

def get_school_info(school, school_id):
    url = 'https://www.switchup.org/chimera/v1/bootcamp-data?mainTemplate=bootcamp-data%2Fdescription&path=%2Fbootcamps%2F'+ str(school) + '&isDataTarget=false&bootcampId='+ str(school_id) + '&logoTag=logo&truncationLength=250&readMoreOmission=...&readMoreText=Read%20More&readLessText=Read%20Less'

    data = requests.get(url).json()

    data.keys()

    courses = data['content']['courses']
    courses_df = pd.DataFrame(courses, columns= ['courses'])

    locations = data['content']['locations']
    locations_df = json_normalize(locations)

    badges_df = pd.DataFrame(data['content']['meritBadges'])
    
    website = data['content']['webaddr']
    description = data['content']['description']
    logoUrl = data['content']['logoUrl']
    school_df = pd.DataFrame([website,description,logoUrl]).T
    school_df.columns =  ['website','description','LogoUrl']

    locations_df['school'] = school
    courses_df['school'] = school
    badges_df['school'] = school
    school_df['school'] = school
    

    locations_df['school_id'] = school_id
    courses_df['school_id'] = school_id
    badges_df['school_id'] = school_id
    school_df['school_id'] = school_id

    return locations_df, courses_df, badges_df, school_df

locations_list = []
courses_list = []
badges_list = []
schools_list = []

for school, id in schools.items():
    print(school)
    a,b,c,d = get_school_info(school,id)
    
    locations_list.append(a)
    courses_list.append(b)
    badges_list.append(c)
    schools_list.append(d)

#data wrangling and cleaning into relational tabular structure
locations = pd.concat(locations_list)
locations.drop(columns= ['country.abbrev','city.keyword','state.id','state.name','state.abbrev','state.keyword','school'], inplace=True)
courses = pd.concat(courses_list)
courses.head(10)
courses.drop(columns=['school'], inplace=True)
courses.reset_index(inplace=True)
courses.reset_index(inplace=True)
courses.drop(columns=['index'], inplace=True)
courses1 = courses.rename(columns = {'level_0':'courses_id'})

badges = pd.concat(badges_list)
badges.drop(columns = ['keyword','description','school'],inplace = True)
badges['badge_id'] = range(1000,len(badges)+1000)
cols = list(badges.columns)
cols.remove('badge_id')
badges = badges[['badge_id']+cols]

schools = pd.concat(schools_list)
schools.head()
schools.drop(columns=['description','LogoUrl'], inplace=True)
cols = list(schools.columns)
cols.remove('school_id')
schools = schools[['school_id']+cols]
comments = comments.merge(schools, left_on='school', right_on='school')
comments.drop(columns=['website','school'], inplace=True)

#insertion of dataframes into mySQL database
#note, populate correct user, pw, and db information for insertion. 
#note below info is placeholder/for demonstration purposes (not real). 

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user='',
                                pw='',
                                db=''))

badges.to_sql('badges',con=engine,if_exists='replace',index = False,chunksize=1000)
schools.to_sql('schools',con=engine,if_exists='replace',index = False,chunksize=1000)
locations.to_sql('locations',con=engine,if_exists='replace',index = False,chunksize=1000)
comments.to_sql('comments',con=engine,if_exists='replace',index = False,chunksize=1000)
courses1.to_sql('courses1',con=engine,if_exists='replace',index = False,chunksize=1000)
