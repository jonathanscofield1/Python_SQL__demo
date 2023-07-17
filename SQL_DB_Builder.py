import sqlite3
from datetime import datetime



#create a database file

con = sqlite3.connect("musicbrainz.db")
cur = con.cursor()

#create the table structure for the database

cur.executescript('''
DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS Area_Alias;
DROP TABLE IF EXISTS Release;    
DROP TABLE IF EXISTS Release_Country;

CREATE TABLE Artist (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    artist_name TEXT,
    area_code INTEGER
);


CREATE TABLE Area_Alias (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    area_code INTEGER,           
    area_name TEXT,
    language TEXT
);


CREATE TABLE Release_Alias(
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    release_code INTEGER,
    release_name  TEXT
);

CREATE TABLE Release_Country(
    id INTEGER,
    country_id INTEGER,
    release_date INTEGER
);
''')

#get data from musicbrainz file

def get_data(filename):
    l = []
    file = open(filename, 'r', encoding = 'utf8')
    for line in file:
        l.append(line.split('\t')) 
    file.close()
    return l

print('loading files...')

artists = get_data('artist.txt')
area_alias = get_data('area_alias.txt')
release_alias = get_data('release_alias.txt')
release_country = get_data('release_country.txt')

print('files loaded successfully')

#load tables

for line in artists:
    try:
        cur.execute('''INSERT OR IGNORE INTO Artist (id, artist_name, area_code)
            VALUES (?, ?, ?)''', (line[0], line[2], line[11]) )
        con.commit()
    except:
        continue

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print('Artists completed at:' + current_time)

#-------------------------------------------

for line in area_alias:
    try:
        cur.execute('''INSERT OR IGNORE INTO Area_Alias (id, area_code, area_name, language)
            VALUES (?, ?, ?, ?)''', (line[0], line[1], line[2], line[3]) )
        con.commit()
    except:
        continue

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print('Area_Alias completed at:' + current_time)

#-------------------------------------------

for line in release_alias:
    try:
        cur.execute('''INSERT OR IGNORE INTO Release_Alias (id, release_code, release_name)
            VALUES (?, ?, ?)''', (line[0], line[1], line[2]) )
        con.commit()
    except:
        continue

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print('Release_Alias completed at:' + current_time)

#-------------------------------------------

for line in release_country:
    try:
        cur.execute('''INSERT OR IGNORE INTO Release_Country (id, country_id, release_date)
            VALUES (?, ?, ?)''', (line[0], line[1], line[2]) )
        con.commit()
    except:
        continue

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print('Release_Country completed at:' + current_time)

print("The script has finished.")
quit()