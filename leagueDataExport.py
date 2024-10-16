import json
import os
from pathlib import Path
import sys
import pandas as pd
from yfpy.query import YahooFantasySportsQuery
import csv

project_dir = Path("D:\\Projects\\yahoo_fantasy_football\\leagueDataExport")
playerListFile = Path(os.path.join(project_dir, 'playerList.csv'))
sys.path.insert(0, str(project_dir))

query = YahooFantasySportsQuery(
    league_id="224437",
    game_code="nfl",
    game_id=449,
    yahoo_consumer_key=os.environ.get("YAHOO_CONSUMER_KEY"),
    yahoo_consumer_secret=os.environ.get("YAHOO_CONSUMER_SECRET"),
    yahoo_access_token_json=os.environ.get("YAHOO_ACCESS_TOKEN_JSON"),
    env_file_location=project_dir,
    save_token_data_to_env_file=True
)

def createPlayerList():
    playerList = query.get_league_players()
    header = ['playerName', 'playerKey']
    with open('playerList.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        for player in playerList:
            playerRow = [player.full_name, player.player_key]
            writer.writerow(playerRow)   

def createTopScoringPlayersList(year, week):
    #Get the data from fantasypros for the specified year and week
    url = "https://www.fantasypros.com/nfl/reports/leaders/half-ppr.php?year=" + str(year) + "&start=" + str(week) + "&end=" + str(week)
    html = pd.read_html(url, header=0)
    df = html[0]
    #get player names and points scored for that week
    topTenPlayerDataFrame = df[["Player", "TTL"]].iloc[:10]
    #add in the manager column to the dataframe
    topTenPlayerDataFrame.insert(2, 'Manager', '', True)
    #create the playerList if it doesn't exist
    if not playerListFile.is_file():
        createPlayerList()
    #create dataframe from the playerList csv
    playerListDataFrame = pd.read_csv(playerListFile, delimiter=',')
    #create dataframe from matching the playerList and the top scoring players, this gives us the players with their playerKey
    playerListDataFrame = playerListDataFrame.loc[playerListDataFrame['playerName'].isin(topTenPlayerDataFrame["Player"])]
    with open('week'+ str(week) + 'TopScoringPlayers.csv', 'w', newline='') as csvfile:
        header = ['playerName', 'Total', 'Manager']
        writer = csv.writer(csvfile)
        writer.writerow(header)
        for index, row in topTenPlayerDataFrame.iterrows():
            #get the player name of the row
            playerName = row['Player']
            #get the player key: search the test dataframe for a match on playerName, get the associated playerKey
            playerKey = (playerListDataFrame.loc[playerListDataFrame['playerName'] == playerName])["playerKey"].values[0]
            #use the get_player_ownership function: access ownership -> owner_team_name
            manager = query.get_player_ownership(playerKey).ownership.owner_team_name
            #If manager doesn't have an owner team name, set manager to Free Agent
            if manager == '':
                manager = 'Free Agent'
            #update the manager column for that row        
            row['Manager'] = manager
            writer.writerow(row)

def createPowerRankingsList(week):
    teamStandings = query.get_league_standings()
    #create the powerRankings csv file for the week
    with open('week'+ str(week) + 'PowerRankings.csv', 'w', newline='') as csvfile:
        header = ['teamName', 'Rank', 'pointsFor', 'pointsAgainst','Change', 'teamID']
        writer = csv.writer(csvfile)
        writer.writerow(header)
        #Open previous week's csv as a dataframe
        previousRankingsFile = Path(os.path.join(project_dir, 'week' +str(week -1) + 'PowerRankings.csv'))
        previousRankingsDataFrame = pd.read_csv(previousRankingsFile, delimiter=',')
        for team in teamStandings.teams:
            #Get previous rank from the csv file, subtract that from the current rank to get the change
            previousRank = previousRankingsDataFrame.loc[previousRankingsDataFrame['teamID'] == team.team_id]['Rank'].values[0]
            currentRank = team.rank
            rankChange = previousRank - currentRank
            #write the values to the current week file
            row = [team.name.decode(), team.rank, team.points_for, team.points_against, rankChange, team.team_id]
            writer.writerow(row)
        
def createTransactionList(week):
    #TODO: Populate start/end times
        #filter transactions based on supplied week
        #Create csv of transactions: team name, player name, action, date
    with open('weekStartEndTimes.txt') as dict_reader:
        weekStartEndTimesDict = json.loads(dict_reader.read())
        print(weekStartEndTimesDict['1']['start'])
    
    #transactionList = query.get_league_transactions()

#createTopScoringPlayersList(2024, 6)        
#createPlayerList()    
#createPowerRankingsList(6)
createTransactionList(6)