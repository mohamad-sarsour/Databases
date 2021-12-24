from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Team(id INTEGER PRIMARY KEY CHECK(id >0)); \
                      CREATE TABLE Match(match_id INTEGER PRIMARY KEY, CHECK(match_id >0), \
                                         competition TEXT NOT NULL, \
                                         homeTeam_id INTEGER NOT NULL, \
                                         awayTeam_id INTEGER NOT NULL, \
                                         FOREIGN KEY(homeTeam_id) REFERENCES Team(id), \
                                         FOREIGN KEY(awayTeam_id) REFERENCES Team(id), \
                                         CHECK(homeTeam_id != awayTeam_id), \
                                         CHECK(competition='International' OR competition='Domestic')); \
                    CREATE TABLE Player(player_id INTEGER PRIMARY KEY CHECK(player_id >0), \
                                         team_id INTEGER NOT NULL CHECK(team_id >0), \
                                         age INTEGER NOT NULL CHECK(age >0), \
                                         height INTEGER NOT NULL CHECK(height >0), \
                                         preferred_foot TEXT NOT NULL ,\
                                         FOREIGN KEY(team_id) REFERENCES Team(id), \
                                         CHECK(preferred_foot='Left' OR preferred_foot='Right')); \
                    CREATE TABLE Stadium(stadium_id INTEGER PRIMARY KEY CHECK(stadium_id >0),\
                                           belong_to INTEGER, \
                                           capacity INTEGER NOT NULL CHECK(capacity >0), \
                                           FOREIGN KEY(belong_to) REFERENCES Team(id), \
                                           UNIQUE(belong_to)); \
                    CREATE TABLE PlayerScores(player_id INTEGER , \
                                                match_id INTEGER , \
                                                goals INTEGER CHECK(goals >0), \
                                                UNIQUE(player_id, match_id), \
 			                                    FOREIGN KEY(player_id) REFERENCES Player(player_id) ON DELETE CASCADE , \
			                                    FOREIGN KEY(match_id) REFERENCES Match(match_id) ON DELETE CASCADE); \
                    CREATE TABLE MatchInStadium(stadium_id INTEGER, \
                                                  match_id INTEGER , \
                                                  attendance INTEGER CHECK(attendance>0), \
                                                  PRIMARY KEY (match_id), \
			                                      FOREIGN KEY(stadium_id) REFERENCES Stadium(stadium_id) ON DELETE CASCADE,\
			                                      FOREIGN KEY(match_id) REFERENCES Match(match_id) ON DELETE CASCADE); \
                    CREATE VIEW active_tall_teams AS\
                            SELECT team_id FROM Player P, Match M\
                            WHERE (team_id=M.homeTeam_id AND P.height > 190)\
                            OR (team_id=M.awayTeam_id AND P.height > 190)\
                            GROUP BY team_id\
                            HAVING COUNT(height)>=2;\
                    CREATE VIEW scores_in_stadium AS\
                            SELECT Stadium.stadium_id, PlayerScores.goals FROM Stadium\
                            LEFT OUTER JOIN MatchInStadium ON(Stadium.stadium_id=MatchInStadium.stadium_id)\
                            FULL JOIN PlayerScores ON(PlayerScores.match_id=MatchInStadium.match_id);\
                    CREATE VIEW all_players_score AS \
                            SELECT Player.player_id, Player.team_id, PlayerScores.match_id, PlayerScores.goals FROM Player \
                            LEFT OUTER JOIN PlayerScores ON(Player.player_id=PlayerScores.player_id);")

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
       conn = Connector.DBConnector()
       conn.execute("DELETE FROM Team;\
                     DELETE FROM Match;\
                     DELETE FROM Player;\
                     DELETE FROM Stadium;\
                     DELETE FROM PlayerScores;\
                     DELETE FROM MatchInStadium;")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Team CASCADE ;\
                      DROP TABLE IF EXISTS Match CASCADE ;\
                      DROP TABLE IF EXISTS Player CASCADE ;\
                      DROP TABLE IF EXISTS Stadium CASCADE;\
                      DROP TABLE IF EXISTS PlayerScores;\
                      DROP TABLE IF EXISTS MatchInStadium;\
                      DROP VIEW IF EXISTS active_tall_teams; \
                      DROP VIEW IF EXISTS scores_in_stadium; \
                      DROP VIEW IF EXISTS all_players_score;")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    #except DatabaseException.UNIQUE_VIOLATION as e:
     #   print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addTeam(teamID: int) -> ReturnValue:
        conn = None
        result = ReturnValue.OK
        try:
            conn = Connector.DBConnector()
            query = sql.SQL("INSERT INTO Team(id) VALUES({id})").format(id=sql.Literal(teamID))

            rows_effected, _ = conn.execute(query)
        except DatabaseException.ConnectionInvalid:
            conn.rollback()
            result = ReturnValue.ERROR
        except DatabaseException.UNIQUE_VIOLATION:
            conn.rollback()
            result = ReturnValue.ALREADY_EXISTS
        except DatabaseException.CHECK_VIOLATION:
            conn.rollback()
            result = ReturnValue.BAD_PARAMS
        except DatabaseException.NOT_NULL_VIOLATION:
            conn.rollback()
            result = ReturnValue.BAD_PARAMS
        except DatabaseException.UNKNOWN_ERROR:
            conn.rollback()
            result = ReturnValue.ERROR
        except Exception:
            result = ReturnValue.ERROR
        finally:
            conn.close()
            return result


def addMatch(match: Match) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        matches = sql.SQL(
            "INSERT INTO Match(match_id,competition,homeTeam_id,awayTeam_id) \
            VALUES({id},{competition},{homeTeam_id},{awayTeam_id});").format(
            id=sql.Literal(match.getMatchID())
            , competition=sql.Literal(match.getCompetition()), homeTeam_id=sql.Literal(match.getHomeTeamID()),
            awayTeam_id=sql.Literal(match.getAwayTeamID()))
        rows_effected, _ = conn.execute(matches)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.rollback()
        result = ReturnValue.ERROR
    except Exception:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def getMatchProfile(matchID: int) -> Match:
    conn = None
    match = Match()
    try:
        conn = Connector.DBConnector()
        matches = sql.SQL("SELECT * FROM Match WHERE Match.match_id = {id}").format(id=sql.Literal(matchID))
        rows_effected, result = conn.execute(matches)
        conn.commit()
        match = Match(result.rows[0][0], result.rows[0][1], result.rows[0][2], result.rows[0][3])
    except DatabaseException.ConnectionInvalid:
        match = Match.badMatch()
    except DatabaseException.CHECK_VIOLATION:
        match = Match.badMatch()
    except DatabaseException.NOT_NULL_VIOLATION:
        match = Match.badMatch()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        match = Match.badMatch()
    except DatabaseException.UNKNOWN_ERROR:
        match = Match.badMatch()
    except Exception:
        match = Match.badMatch()
    finally:
        conn.close()
        return match


def deleteMatch(match: Match) -> ReturnValue:
    conn = None
    result, rows_effected = ReturnValue.OK, None
    try:
        conn = Connector.DBConnector()
        matches = sql.SQL(
            "DELETE FROM Match WHERE Match.match_id={id} AND Match.homeTeam_id={homeTeam_id} AND Match.awayTeam_id={awayTeam_id} AND Match.competition={competition}").format(
            id=sql.Literal(match.getMatchID()), homeTeam_id=sql.Literal(match.getHomeTeamID()), competition=sql.Literal(match.getCompetition()),
            awayTeam_id=sql.Literal(match.getAwayTeamID()))
        rows_effected, _ = conn.execute(matches)
        if rows_effected is not None and rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR

    finally:
        conn.close()
        return result


def addPlayer(player: Player) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        players = sql.SQL(
            "INSERT INTO Player(player_id,team_id,age,height,preferred_foot) VALUES({id},{team_id},{age},{height},{preferred_foot});").format(
            id=sql.Literal(player.getPlayerID())
            , team_id=sql.Literal(player.getTeamID()), age=sql.Literal(player.getAge()),
            height=sql.Literal(player.getHeight()), preferred_foot=sql.Literal(player.getFoot()))
        rows_effected, _ = conn.execute(players)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.rollback()
        result = ReturnValue.ERROR
    except Exception:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def getPlayerProfile(playerID: int) -> Player:
    conn = None
    player = Player()
    try:
        conn = Connector.DBConnector()
        players = sql.SQL("SELECT * FROM Player WHERE Player.player_id = {id}").format(id=sql.Literal(playerID))
        rows_effected, result = conn.execute(players)
        conn.commit()
        player = Player(result.rows[0][0], result.rows[0][1], result.rows[0][2], result.rows[0][3], result.rows[0][4])
    except DatabaseException.ConnectionInvalid:
        player = Player.badPlayer()
    except DatabaseException.CHECK_VIOLATION:
        player = Player.badPlayer()
    except DatabaseException.NOT_NULL_VIOLATION:
        player = Player.badPlayer()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        player = Player.badPlayer()
    except DatabaseException.UNKNOWN_ERROR:
        player = Player.badPlayer()
    except Exception:
        player = Player.badPlayer()
    finally:
        conn.close()
        return player


def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    result, rows_effected = ReturnValue.OK, None
    try:
        conn = Connector.DBConnector()
        players = sql.SQL("DELETE FROM Player WHERE Player.player_id={id} AND Player.team_id={team_id}").format(
            id=sql.Literal(player.getPlayerID())
            , team_id=sql.Literal(player.getTeamID()))
        rows_effected, _ = conn.execute(players)
        if rows_effected is not None and rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR

    finally:
        conn.close()
        return result


def addStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        stadiums = sql.SQL(
            "INSERT INTO Stadium(stadium_id,capacity,belong_to) VALUES({id},{capacity},{belong_to});").format(
            id=sql.Literal(stadium.getStadiumID())
            , capacity=sql.Literal(stadium.getCapacity()), belong_to=sql.Literal(stadium.getBelongsTo()))
        rows_effected, _ = conn.execute(stadiums)
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.rollback()
        result = ReturnValue.ERROR
    except Exception:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def getStadiumProfile(stadiumID: int) -> Stadium:
    conn = None
    try:
        conn = Connector.DBConnector()
        stadiums = sql.SQL("SELECT * FROM Stadium WHERE Stadium.stadium_id = {id}").format(id=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(stadiums)
        conn.commit()
        stadium = Stadium(result.rows[0][0], result.rows[0][2], result.rows[0][1])
    except DatabaseException.ConnectionInvalid:
        stadium = Stadium.badStadium()
    except DatabaseException.CHECK_VIOLATION:
        stadium = Stadium.badStadium()
    except DatabaseException.NOT_NULL_VIOLATION:
        stadium = Stadium.badStadium()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        stadium = Stadium.badStadium()
    except DatabaseException.UNKNOWN_ERROR:
        stadium = Stadium.badStadium()
    except Exception:
        stadium = Stadium.badStadium()
    finally:
        conn.close()
        return stadium


def deleteStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    result, rows_effected = ReturnValue.OK, None
    try:
        conn = Connector.DBConnector()
        stadiums = sql.SQL("DELETE FROM Stadium WHERE Stadium.stadium_id={id} AND Stadium.capacity={capacity} AND Stadium.belong_to={belong_to}").format(
            id=sql.Literal(stadium.getStadiumID()), capacity=sql.Literal(stadium.getCapacity()), belong_to=sql.Literal(stadium.getBelongsTo()))
        rows_effected,_ = conn.execute(stadiums)
        if rows_effected is not None and rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR

    finally:
        conn.close()
        return result


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        playersScores = sql.SQL(
            "INSERT INTO PlayerScores(player_id,match_id,goals) VALUES({id},{match_id},{goals});").format(
            id=sql.Literal(player.getPlayerID())
            , match_id=sql.Literal(match.getMatchID()), goals=sql.Literal(amount)
        )
        rows_effected, _ = conn.execute(playersScores)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION or DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR or Exception:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    conn = None
    result, rows_effected = ReturnValue.OK, None
    try:
        conn = Connector.DBConnector()
        playerscore = sql.SQL(
            "Delete FROM PlayerScores WHERE PlayerScores.player_id = {id} AND PlayerScores.match_id={match_id} ").format(
            id=sql.Literal(player.getPlayerID()),
            match_id=sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(playerscore)
        conn.commit()
        if rows_effected is not None and rows_effected == 0:
            result = ReturnValue.NOT_EXISTS

    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    sid = stadium.getStadiumID()
    mid = match.getMatchID()
    try:
        conn = Connector.DBConnector()
        matchesStaduim = sql.SQL(
            "INSERT INTO MatchInStadium(stadium_id,match_id,attendance) VALUES({id},{match_id},{atten});").format(
            id=sql.Literal(sid), match_id=sql.Literal(mid), atten=sql.Literal(attendance) )
        rows_effected, _ = conn.execute(matchesStaduim)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION or DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNKNOWN_ERROR or Exception as e:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    conn = None
    result, rows_effected = ReturnValue.OK, None
    try:
        conn = Connector.DBConnector()
        match_not_in = sql.SQL(
            "DELETE FROM MatchInStadium WHERE MatchInStadium.stadium_id = {id} AND MatchInStadium.match_id={match_id} ").format(
            id=sql.Literal(stadium.getStadiumID()),
            match_id=sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(match_not_in)
        conn.commit()
        if rows_effected is not None and rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def averageAttendanceInStadium(stadiumID: int) -> float:
    conn = None
    result, rows_effected =0, None
    try:
        conn = Connector.DBConnector()
        averageAttendance = sql.SQL(
            "SELECT  AVG(attendance) FROM MatchInStadium WHERE stadium_id={ss} ").format(
            ss=sql.Literal(stadiumID))
        rows_effected, tuples = conn.execute(averageAttendance)
        conn.commit()
        if tuples.rows[0][0] is None or len(tuples.rows) == 0:
            result = 0
        else:
            result = tuples.rows[0][0]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        result = -1
    finally:
        conn.close()
        return result


def stadiumTotalGoals(stadiumID: int) -> int:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COALESCE(SUM(goals),0) FROM PlayerScores \
                         WHERE match_id IN \
                         (SELECT MatchInStadium.match_id FROM MatchInStadium \
                         WHERE MatchInStadium.stadium_id={0})").format(sql.Literal(stadiumID))
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            result = tuples.rows[0][0]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        result = -1
    conn.close()
    return result


def playerIsWinner(playerID: int, matchID: int) -> bool:
    conn = None
    result = False
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT player_id\
                         FROM PlayerScores \
                         WHERE (player_id={PID}) AND (match_id={MID}) \
                         GROUP BY player_id \
                         HAVING (2*COALESCE(SUM(goals),0)>=(SELECT COALESCE(SUM(goals),0) FROM PlayerScores WHERE match_id={MID})) \
                         AND (COALESCE(SUM(goals),0)!=0);").format(PID=sql.Literal(playerID), MID=sql.Literal(matchID))
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            result = True
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        result = False
    conn.close()
    return result


def getActiveTallTeams() -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT team_id FROM active_tall_teams ORDER BY team_id DESC LIMIT 5")
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
             res = [r[0] for r in tuples.rows]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        conn.close()
        return []
    conn.close()
    return res


def getActiveTallRichTeams() -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT team_id FROM active_tall_teams \
                         WHERE team_id IN (SELECT belong_to FROM Stadium WHERE capacity>55000) \
                         ORDER BY team_id ASC LIMIT 5")
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            res = [r[0] for r in tuples.rows]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        conn.close()
        return []
    conn.close()
    return res


def popularTeams() -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT T.homeTeam_id FROM  \
                            (SELECT Match.homeTeam_id, MatchInStadium.attendance FROM \
                            Match LEFT OUTER JOIN MatchInStadium ON(Match.match_id=MatchInStadium.match_id)) T\
                         GROUP BY T.homeTeam_id \
                         HAVING MIN(COALESCE(T.attendance,0)) > 40000 \
                         ORDER BY T.homeTeam_id DESC LIMIT 10;")
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            res = [r[0] for r in tuples.rows]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        conn.close()
        return []
    conn.close()
    return res


def getMostAttractiveStadiums() -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT stadium_id ,COALESCE(SUM(goals),0) AS scores \
                         FROM scores_in_stadium \
                         GROUP BY stadium_id \
                         ORDER BY scores DESC, stadium_id ASC;")
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            res = [r[0] for r in tuples.rows]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        conn.close()
        return []
    conn.close()
    return res


def mostGoalsForTeam(teamID: int) -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT player_id, COALESCE(SUM(goals),0) AS scores \
                         FROM all_players_score \
                         WHERE team_id={0} \
                         GROUP BY player_id \
                         ORDER BY scores DESC, player_id DESC LIMIT 5;").format(sql.Literal(teamID))
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            res = [r[0] for r in tuples.rows]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        conn.close()
        return []
    conn.close()
    return res


def getClosePlayers(playerID: int) -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AP.player_id \
                         FROM all_players_score AP \
                         WHERE AP.player_id!={0} \
                         AND AP.match_id IN(SELECT match_id FROM PlayerScores WHERE player_id={0}) \
                         GROUP BY AP.player_id \
                         HAVING 2*COUNT(AP.match_id) >= (SELECT COUNT(match_id) FROM PlayerScores WHERE player_id={0}) \
                         ORDER BY player_id ASC LIMIT 10;").format(sql.Literal(playerID))
        effected_rows, tuples = conn.execute(query)
        if len(tuples.rows) != 0:
            res = [r[0] for r in tuples.rows]
    except DatabaseException.ConnectionInvalid or DatabaseException.UNKNOWN_ERROR or Exception:
        conn.close()
        return []
    conn.close()
    return res
