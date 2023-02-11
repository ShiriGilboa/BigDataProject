from dataclasses import dataclass


@dataclass
class GameScore:
    def __init__(self, GameID, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK,
                 TournOvers, PTS):
        self.GameID = GameID
        self.FGA = FGA
        self.FGM = FGM
        self.FGPER = FGPER
        self.ThreePA = ThreePA
        self.ThreePM = ThreePM
        self.ThreePPER = ThreePPER
        self.FTA = FTA
        self.FTM = FTM
        self.FTPER = FTPER
        self.OREB = OREB
        self.DREB = DREB
        self.AST = AST
        self.STL = STL
        self.BLK = BLK
        self.TournOvers = TournOvers
        self.PTS = PTS

    def __eq__(self, other):
        if not isinstance(other, GameScore):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.GameID == other.GameID

    def __str__(self):
        return f'GameID: {self.GameID}, FGA: {self.FGA}, FGM: {self.FGM}, FGPER: {self.FGPER}, ThreePM: {self.ThreePM}, ThreePA: {self.ThreePA}, ThreePPER: {self.ThreePPER}, FTM: {self.FTM}, FTA: {self.FTA}, FTPER: {self.FTPER}, OREB: {self.OREB}, DREB: {self.DREB}, AST: {self.AST}, STL: {self.STL}, BLK: {self.BLK}, TournOvers: {self.TournOvers}, PTS: {self.PTS}'


@dataclass
class GameTeamScore(GameScore):
    def __init__(self, GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER, ThreePA, ThreePM,
                 ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS):
        self.TeamID = TeamID
        self.ScoreQOne = ScoreQOne
        self.ScoreQTwo = ScoreQTwo
        self.ScoreQThree = ScoreQThree
        self.ScoreQFour = ScoreQFour
        super().__init__(GameID, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL,
                         BLK, TournOvers, PTS)

    def __str__(self):
        return f'''TeamID: {self.TeamID}, Q1: {self.ScoreQOne}, Q2: {self.ScoreQTwo}, Q3: {self.ScoreQThree}, Q4: {self.ScoreQFour}, {super().__str__()}'''


@dataclass
class GamePlayerScore(GameScore):
    def __init__(self, GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER,
                 FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS):
        self.PlayerID = PlayerID
        self.PlayTimeInSec = PlayTimeInSec
        self.PlusMinusRank = PlusMinusRank
        super().__init__(GameID, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL,
                         BLK, TournOvers, PTS)

    def __str__(self):
        return f'''PlayerID: {self.PlayerID}, PlayTimeInSec: {self.PlayTimeInSec}, PlusMinusRank: {self.PlusMinusRank}, {super().__str__()}'''


@dataclass
class Game:
    def __init__(self, ID, Date, HomeTeamID, VisitorTeamID, LiveStatus):
        self.ID = ID
        self.Date = Date
        self.HomeTeamID = HomeTeamID
        self.VisitorTeamID = VisitorTeamID
        self.LiveStatus = LiveStatus

    def __eq__(self, other):
        if not isinstance(other, Game):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.ID == other.ID and self.LiveStatus == other.LiveStatus

    def __str__(self):
        return f'{self.ID} {self.Date} {self.HomeTeamID} {self.VisitorTeamID} {self.LiveStatus}'


@dataclass
class GameLineUp:
    def __init__(self, GameID: str, TeamID: str, PlayerID: str):
        self.GameID = GameID
        self.TeamID = TeamID
        self.PlayerID = PlayerID

    def __eq__(self, other):
        if not isinstance(other, GameLineUp):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.GameID == other.GameID and self.TeamID == other.TeamID and self.PlayerID == other.PlayerID

    def __str__(self):
        return f'{self.GameID} {self.TeamID} {self.PlayerID}'


@dataclass
class Player:
    def __init__(self, ID, FirstName, LastName, DateOfBirth, Position, Height, Weight, YearsInLeague, Country):
        self.ID = ID
        self.FirstName = FirstName
        self.LastName = LastName
        self.DateOfBirth = DateOfBirth
        self.Position = Position
        self.Height = Height
        self.Weight = Weight
        self.YearsInLeague = YearsInLeague
        self.Country = Country

    def __eq__(self, other):
        if not isinstance(other, Player):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.ID == other.ID and self.Country == other.Country and self.YearsInLeague == other.YearsInLeague

    def __str__(self):
        return f"ID: {self.ID}, Name: {self.FirstName} {self.LastName}, DOB: {self.DateOfBirth}, Position: {self.Position}, Height: {self.Height}, Weight: {self.Weight}, Years in League: {self.YearsInLeague}, Country: {self.Country}"


@dataclass
class Team:
    def __init__(self, ID: int, name: str, code: str, city: str):
        self.ID = ID
        self.TeamName = name
        self.TeamCode = code
        self.City = city

    def __eq__(self, other):
        if not isinstance(other, Team):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.ID == other.ID and self.TeamName == other.TeamName and self.City == other.City

    def __str__(self):
        return f'{self.ID} {self.TeamName} {self.TeamCode} {self.City}'


@dataclass
class TeamRoster:
    def __init__(self, TeamID, PlayerID):
        self.TeamID = TeamID
        self.PlayerID = PlayerID

    def __eq__(self, other):
        if not isinstance(other, TeamRoster):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.TeamID == other.TeamID and self.PlayerID == other.PlayerID

    def __str__(self):
        return f'{self.TeamID} {self.PlayerID}'
