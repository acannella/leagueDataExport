from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
import pandas as pd
from yfpy.query import YahooFantasySportsQuery
import csv
from dateutil.parser import parse
import os
from dotenv import load_dotenv

load_dotenv()

project_dir = Path(os.getenv("PROJECT_DIR"))
playerListFile = Path(os.path.join(project_dir, 'playerList.csv'))
sys.path.insert(0, str(project_dir))

suppliedYear = sys.argv[1]
suppliedWeek = sys.argv[2]

query = YahooFantasySportsQuery(
    league_id=os.getenv("LEAGUE_ID"),
    game_code="nfl",
    game_id=449,
    yahoo_access_token_json=os.getenv("YAHOO_ACCESS_TOKEN_JSON"),
    env_file_location=project_dir,
    save_token_data_to_env_file=True
)

def createPlayerList():
    playerList = query.get_league_players()
    header = ['Player Name', 'Player Key']
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
    #get top 10 player names and points scored for that week
    topTenPlayerDataFrame = df[["Player", "TTL"]].iloc[:10]
    #add in the manager column to the dataframe
    topTenPlayerDataFrame.insert(2, 'Manager', '', True)
    #create the playerList if it doesn't exist
    if not playerListFile.is_file():
        createPlayerList()
    #create dataframe from the playerList csv
    playerListDataFrame = pd.read_csv(playerListFile, delimiter=',')
    #create dataframe from matching the playerList and the top scoring players, this gives us the players with their playerKey
    playerListDataFrame = playerListDataFrame.loc[playerListDataFrame['Player Name'].isin(topTenPlayerDataFrame["Player"])]
    with open('week'+ str(week) + 'TopScoringPlayers.csv', 'w', newline='') as csvfile:
        header = ['Player Name', 'Fantasy Points', 'Manager']
        writer = csv.writer(csvfile)
        writer.writerow(header)
        for index, row in topTenPlayerDataFrame.iterrows():
            #get the player name of the row
            playerName = row['Player']
            #get the player key: search the test dataframe for a match on playerName, get the associated playerKey
            playerKey = (playerListDataFrame.loc[playerListDataFrame['Player Name'] == playerName])["Player Key"].values[0]
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
        header = ['Rank', 'Team Name', 'Change', 'Record','Points For', 'Points Against', 'Team ID']
        writer = csv.writer(csvfile)
        writer.writerow(header)
        #Open previous week's csv as a dataframe
        previousRankingsFile = Path(os.path.join(project_dir, 'week' + str(int(week) - 1) + 'PowerRankings.csv'))
        previousRankingsDataFrame = pd.read_csv(previousRankingsFile, delimiter=',')
        for team in teamStandings.teams:
            #Get previous rank from the csv file, subtract that from the current rank to get the change
            previousRank = previousRankingsDataFrame.loc[previousRankingsDataFrame['Team ID'] == team.team_id]['Rank'].values[0]
            currentRank = team.rank
            rankChange = previousRank - currentRank
            wins = team.team_standings.outcome_totals.wins
            losses = team.team_standings.outcome_totals.losses
            ties = team.team_standings.outcome_totals.ties
            record = str(wins) + '-' + str(losses) + '-' + str(ties)
            #write the values to the current week file
            row = [team.rank, team.name.decode(), rankChange, record, round(team.points_for,2), round(team.points_against,2), team.team_id]
            writer.writerow(row)
        
def createTransactionList(week):
    #Get the end date for the specified week, use parse to get datetime obj from that date, add 1 day to the end date to capture all transactions that week
    weekEndDate = (parse(query.get_league_matchups_by_week(week)[0].week_end)) + timedelta(days=1)
    #Transactions have UNIX timestamp so convert the week to UNIX value for comparison
    weekEndDateUNIX = weekEndDate.timestamp()
    #Get the start date for the specified week, use parse to get datetime obj from that date, add 1 day to the start date to capture all transactions that week
    weekStartDate = (parse(query.get_league_matchups_by_week(week)[0].week_start)) + timedelta(days=1)
    weekStartDateUNIX = weekStartDate.timestamp()
    transactionList = query.get_league_transactions()
    #Create new list of transactions filtered on the timestamp being >= week start date and <= week end date
    filteredTransactionList = list(filter(lambda x: x.timestamp >= weekStartDateUNIX and x.timestamp <= weekEndDateUNIX, transactionList))
    with open('week'+ str(week) + 'Transactions.csv', 'w', newline='') as csvfile:
        writer =csv.writer(csvfile)
        header = ['Team Name', 'Action', 'Player Name', 'transactionType', 'Date']
        writer.writerow(header)
        for transaction in filteredTransactionList:
            for player in transaction.players:
                #Convert to readable date from unix timestamp
                transactionDate = (datetime.fromtimestamp(transaction.timestamp)).strftime('%b %d %Y %H:%M:%S')
                #Get the team name that is associated with the transaction
                teamName = player.transaction_data.source_team_name if player.transaction_data.source_team_name != '' else player.transaction_data.destination_team_name
                #Get the action related to the specific player: add, drop, or trade
                playerActionType = player.transaction_data.type
                playerName = player.full_name
                #Get the overall transaction type, this is mainly for capturing when a player is added and another dropped within the same transaction
                transactionType = transaction.type
                row = [teamName, playerActionType, playerName, transactionType, transactionDate]
                writer.writerow(row)

#createPlayerList() 
createTopScoringPlayersList(suppliedYear, suppliedWeek)           
createPowerRankingsList(suppliedWeek)
createTransactionList(suppliedWeek)