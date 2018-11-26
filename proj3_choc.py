import sqlite3
import csv
import json
import os
import pprint

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

# Drop Tables if need
statement = '''
    DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)

statement = '''
    DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)

conn.commit()



# Create tables
statement = '''
    CREATE TABLE 'Countries' (
        'id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT,
        'Alpha3' TEXT,
        'EnglishName' TEXT,
        'Region' TEXT,
        'Subregion' TEXT,
        'Population' INTEGER,
        'Area' REAL             
    );
'''
cur.execute(statement)


statement = '''
    CREATE TABLE 'Bars' (
        'id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT,
        'SpecificBeanBarName' TEXT,
        'REF' TEXT,
        'ReviewDate' TEXT,
        'CocoaPercent' REAL,
        'CompanyLocationId' INTEGER,         
        'Rating' REAL,
        'BeanType' TEXT,
        'BroadBeanOriginId' INTEGER,
        FOREIGN KEY ('CompanyLocationId') REFERENCES Countries('Id'),  
        FOREIGN KEY ('BroadBeanOriginId') REFERENCES Countries('Id')              
    );
'''
cur.execute(statement)


with open(COUNTRIESJSON,'rb') as f:
    countries=json.loads(f.read())
    for country in countries:
        statement= '''
            INSERT INTO 'Countries' (Alpha2, Alpha3,EnglishName,Region,Subregion,Population,Area) VALUES (?,?,?,?,?,?,?)
        '''
        cur.execute(statement, (country['alpha2Code'],country['alpha3Code'],country['name'],country['region'],country['subregion'],country['population'],country['area']))



with open(BARSCSV, encoding='utf-8') as f:
    csvReader = csv.reader(f)         
    for row in csvReader:
        if row[0]== 'Company':
            continue

        cur.execute("SELECT id FROM {} WHERE EnglishName=?".format('Countries'), (row[5],))
        CompanyLocationId=int(cur.fetchone()[0])
        # print(CompanyLocationId)
        cur.execute("SELECT id FROM {} WHERE EnglishName=?".format('Countries'), (row[8],))
        BroadBeanOriginTuple=cur.fetchone()
        if BroadBeanOriginTuple==None:
            BroadBeanOriginId="NULL"
        else:
            BroadBeanOriginId=int(BroadBeanOriginTuple[0])    
        # print(BroadBeanOriginId)

        statement= '''
            INSERT INTO 'Bars' (Company, SpecificBeanBarName,REF,ReviewDate,CocoaPercent,CompanyLocationId,Rating,BeanType,BroadBeanOriginId) VALUES (?,?,?,?,?,?,?,?,?)
        '''
        cur.execute(statement, (row[0], row[1],row[2],row[3],float(row[4][:-1])/100,CompanyLocationId,row[6],row[7],BroadBeanOriginId))




conn.commit()
conn.close()
# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    command_split=command.split()
    if command_split[0]=='bars':
        sql = '''
            SELECT b.SpecificBeanBarName,b.Company,c1.EnglishName,ROUND(b.Rating,1),ROUND(b.CocoaPercent,2),c2.EnglishName
            FROM Bars AS b
                JOIN Countries AS c1
                ON b.CompanyLocationId=c1.Id
                LEFT JOIN Countries AS c2
                ON b.BroadBeanOriginId=c2.Id
        '''
        
        for one_command in command_split:
            if 'sellcountry' in one_command:
                sql+='WHERE c1.Alpha2=\''+one_command[-2:]+'\' '
            elif 'sourcecountry' in one_command:
                sql+='WHERE c2.Alpha2=\''+one_command[-2:]+'\' '
            elif 'sellregion' in one_command:
                sql+='WHERE c1.Region=\''+one_command[11:]+'\' '
            elif 'sourceregion' in one_command:
                sql+='WHERE c2.Region=\''+one_command[13:]+'\' '
            
        if 'cocoa' in command:
            sql+='ORDER BY b.CocoaPercent '
        else:
            sql+='ORDER BY b.Rating '
        flag=0
        for one_command in command_split:
            if 'top' in one_command:
                sql+='DESC LIMIT '+one_command[4:]
                flag=1
            elif 'bottom' in one_command:
                sql+='ASC LIMIT '+one_command[7:]
                flag=1
        if flag==0:
            sql+='DESC LIMIT 10'
        
        results = cur.execute(sql)
        result_list = results.fetchall()
    
    if command_split[0]=='companies':
        sql = '''
            SELECT b.Company,c1.EnglishName,
        '''
            
        if 'cocoa' in command:
            sql+='ROUND(AVG(b.CocoaPercent),2) '
            rate='ORDER BY AVG(b.CocoaPercent) '
        elif 'bars_sold' in command:
            sql+='COUNT(b.Company)'
            rate='ORDER BY COUNT(b.Company) '
        else:
            sql+='ROUND(AVG(b.Rating),1)'
            rate='ORDER BY AVG(b.Rating) '


        sql+='''
            FROM Bars AS b
                JOIN Countries AS c1
                ON b.CompanyLocationId=c1.Id
                LEFT JOIN Countries AS c2
                ON b.BroadBeanOriginId=c2.Id
        '''

        for one_command in command_split:
            if 'country' in one_command:
                sql+='WHERE c1.Alpha2=\''+one_command[-2:]+'\' '
            elif 'region' in one_command:
                sql+='WHERE c1.Region=\''+one_command[7:]+'\' '
        sql+='''            
            GROUP BY b.Company
            HAVING COUNT(b.Company)>4
            '''

        sql+=rate
        flag=0
        for one_command in command_split:
            if 'top' in one_command:
                sql+='DESC LIMIT '+one_command[4:]
                flag=1
            elif 'bottom' in one_command:
                sql+='ASC LIMIT '+one_command[7:]
                flag=1
        if flag==0:
            sql+='DESC LIMIT 10'
        
        results = cur.execute(sql)
        result_list = results.fetchall()


    if command_split[0]=='countries':
        if 'sources' in command:            
            sql = '''
                SELECT c2.EnglishName,c2.Region,
            '''                
            if 'cocoa' in command:
                sql+='ROUND(AVG(b.CocoaPercent),2) '
                rate='ORDER BY AVG(b.CocoaPercent) '
            elif 'bars_sold' in command:
                sql+='COUNT(b.BroadBeanOriginId)'
                rate='ORDER BY COUNT(b.BroadBeanOriginId) '
            else:
                sql+='ROUND(AVG(b.Rating),1)'
                rate='ORDER BY AVG(b.Rating) '

            sql+='''
                FROM Bars AS b
                    LEFT JOIN Countries AS c1
                    ON b.CompanyLocationId=c1.Id
                    LEFT JOIN Countries AS c2
                    ON b.BroadBeanOriginId=c2.Id
            '''

            for one_command in command_split:
                if 'region' in one_command:
                    sql+='WHERE c2.Region=\''+one_command[7:]+'\' '
            sql+='''            
                GROUP BY b.BroadBeanOriginId
                HAVING COUNT(b.BroadBeanOriginId)>4
                '''
        else:            
            sql = '''
                SELECT c1.EnglishName,c1.Region,
            '''                
            if 'cocoa' in command:
                sql+='ROUND(AVG(b.CocoaPercent),2) '
                rate='ORDER BY AVG(b.CocoaPercent) '
            elif 'bars_sold' in command:
                sql+='COUNT(b.CompanyLocationId)'
                rate='ORDER BY COUNT(b.CompanyLocationId) '
            else:
                sql+='ROUND(AVG(b.Rating),1)'
                rate='ORDER BY AVG(b.Rating) '


            sql+='''
                FROM Bars AS b
                    LEFT JOIN Countries AS c1
                    ON b.CompanyLocationId=c1.Id
                    LEFT JOIN Countries AS c2
                    ON b.BroadBeanOriginId=c2.Id
            '''

            for one_command in command_split:
                if 'region' in one_command:
                    sql+='WHERE c1.Region=\''+one_command[7:]+'\' '
            sql+='''            
                GROUP BY b.CompanyLocationId
                HAVING COUNT(b.CompanyLocationId)>4
                '''

        sql+=rate
        flag=0
        for one_command in command_split:
            if 'top' in one_command:
                sql+='DESC LIMIT '+one_command[4:]
                flag=1
            elif 'bottom' in one_command:
                sql+='ASC LIMIT '+one_command[7:]
                flag=1
        if flag==0:
            sql+='DESC LIMIT 10'

        
        results = cur.execute(sql)
        result_list = results.fetchall()


    if command_split[0]=='regions':
        if 'sources' in command:            
            sql = '''
                SELECT c2.Region,
            '''                
            if 'cocoa' in command:
                sql+='ROUND(AVG(b.CocoaPercent),2) '
                rate='ORDER BY AVG(b.CocoaPercent) '
            elif 'bars_sold' in command:
                sql+='COUNT(b.BroadBeanOriginId)'
                rate='ORDER BY COUNT(b.BroadBeanOriginId) '
            else:
                sql+='ROUND(AVG(b.Rating),1)'
                rate='ORDER BY AVG(b.Rating) '


            sql+='''
                FROM Bars AS b
                    LEFT JOIN Countries AS c1
                    ON b.CompanyLocationId=c1.Id
                    LEFT JOIN Countries AS c2
                    ON b.BroadBeanOriginId=c2.Id
            '''

            
            sql+='''            
                GROUP BY c2.Region
                HAVING COUNT(c2.Region)>4
                '''
        else:            
            sql = '''
                SELECT c1.Region,
            '''                
            if 'cocoa' in command:
                sql+='ROUND(AVG(b.CocoaPercent),2) '
                rate='ORDER BY AVG(b.CocoaPercent) '
            elif 'bars_sold' in command:
                sql+='COUNT(b.CompanyLocationId)'
                rate='ORDER BY COUNT(b.CompanyLocationId) '
            else:
                sql+='ROUND(AVG(b.Rating),1)'
                rate='ORDER BY AVG(b.Rating) '


            sql+='''
                FROM Bars AS b
                    LEFT JOIN Countries AS c1
                    ON b.CompanyLocationId=c1.Id
                    LEFT JOIN Countries AS c2
                    ON b.BroadBeanOriginId=c2.Id
            '''

            
            sql+='''            
                GROUP BY c1.Region
                HAVING COUNT(c1.Region)>4
                '''

        sql+=rate
        flag=0
        for one_command in command_split:
            if 'top' in one_command:
                sql+='DESC LIMIT '+one_command[4:]
                flag=1
            elif 'bottom' in one_command:
                sql+='ASC LIMIT '+one_command[7:]
                flag=1
        if flag==0:
            sql+='DESC LIMIT 10'




        
        results = cur.execute(sql)
        result_list = results.fetchall()
    



    # print(sql)
    # print(result_list)
    conn.close()
    return result_list


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue
        elif response=='':
            print('')
            continue

        elif response[:4]=='bars':
            # print(1)
            if len(response)>4 and ('sellcountry=' not in response) and ('sourcecountry=' not in response) and ('sellregion=' not in response) and ('sourceregion='not in response) and ('ratings'not in response) and ('cocoa'not in response) and ('bottom='not in response) and ('top='not in response):
                print('Command not recognized: '+ response)
                print('')
                continue
            results=process_command(response)
            for result in results:
                if result[5] == None:
                    print('{:40}'.format(result[0]),'{:25}'.format(result[1]),'{:25}'.format(result[2]),'{:5}'.format(str(round(float(result[3]),1))),'{:5}'.format(str(float(result[4])*100)[:2]+"%"),'{:10}'.format('Unknown'))
                else:
                    print('{:40}'.format(result[0]),'{:25}'.format(result[1]),'{:25}'.format(result[2]),'{:5}'.format(str(round(float(result[3]),1))),'{:5}'.format(str(float(result[4])*100)[:2]+"%"),'{:10}'.format(result[5]))
            print('')
            continue
        
        
        elif 'companies' in response:
            # print(2)
            
            if len(response)>9 and ('country='not in response) and ('region=' not in response) and ('bars_sold' not in response) and ('ratings' not in response) and ('cocoa' not in response) and ('bottom=' not in response) and ('top=' not in response):
                print('Command not recognized: '+ response)
                print('')
                continue
            results=process_command(response)
            for result in results:
                print('{:50}'.format(result[0]),'{:55}'.format(result[1]),'{:5}'.format(result[2]))
            print('')
            continue
        
        
        elif 'countries' in response:
            # print(3)
            if len(response)>9 and ('sellers'not in response) and ('sources'not in response) and ('region='not in response) and ('bars_sold'not in response) and ('ratings'not in response) and ('cocoa'not in response) and ('bottom='not in response) and ('top='not in response):
                print('Command not recognized: '+ response)
                print('')
                continue
            results=process_command(response)
            for result in results:
                print('{:55}'.format(result[0]),'{:25}'.format(result[1]),'{:5}'.format(result[2]))
            print('\n')
            continue
        
        
        
        elif 'regions' in response:
            # print(4)
            if len(response)>7 and ('sellers' not in response) and ('sources' not in response)  and ('bars_sold'not in response) and ('ratings'not in response) and ('cocoa'not in response) and ('bottom=' not in response) and ('top='not in response):
                print('Command not recognized: '+ response)
                print('')
                continue
            results=process_command(response)
            for result in results:
                print('{:40}'.format(result[0]),'{:5}'.format(result[1]))
            print('')
            continue


        elif response == 'exit':
            print('bye')
            break
        else:  
            print('Command not recognized: '+ response)
            print('')
    


# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
    # process_command('bars nothing')
    