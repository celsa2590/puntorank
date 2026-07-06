from pydantic import BaseModel

class PlayerCreate(BaseModel):
    name: str
    club_id: int | None = None
    is_registered: bool = False
    side: str | None = None

class PlayerRegister(BaseModel):
    name: str
    email: str
    club_id: int | None = None
    gender: str | None = None
    side: str | None = None
    category: str | None = None

class MatchPlayer(BaseModel):
    player_id: int | None = None
    name: str | None = None
    side: str | None = None
    team: str  # A o B


class MatchReport(BaseModel):
    club_id: int
    event_id: int | None = None
    match_type: str = "standard"
    created_by: int | None = None
    played_at: str | None = None
    score: str
    winning_team: str
    players: list[MatchPlayer]
    category: str | None = None

class ClubLogin(BaseModel):
    username: str
    password: str

class AmericanoCreate(BaseModel):
    club_id: int
    name: str
    category: str
    gender: str
    courts: int
    duration_minutes: int

class LeagueCreate(BaseModel):
    club_id: int
    name: str
    category: str
    gender: str
    format: str = "round_robin"
    start_date: str | None = None
    end_date: str | None = None

class AmericanoAddPlayer(BaseModel):
    player_id: int | None = None
    name: str | None = None
    email: str | None = None
    gender: str | None = None
    category: str | None = None
    side: str | None = None

class AmericanoPairCreate(BaseModel):
    player_1_id: int
    player_2_id: int
    pair_name: str | None = None

class AmericanoMatchResult(BaseModel):
    pair_a_games: int
    pair_b_games: int

class LeaguePairCreate(BaseModel):
    player_1_id: int
    player_2_id: int
    pair_name: str | None = None
class LeagueMatchResult(BaseModel):
    score: str
    winner_pair_id: int


class TournamentCreate(BaseModel):
    club_id: int
    name: str
    category: str
    gender: str

class TournamentPairCreate(BaseModel):
    player_1_id: int
    player_2_id: int
    pair_name: str | None = None
    payment_status: str = "pending"
    payment_method: str | None = None
    payment_link: str | None = None
    payment_reference: str | None = None
    payment_amount: int | None = None

class TournamentPaymentUpdate(BaseModel):
    payment_status: str
    payment_method: str | None = None
    payment_link: str | None = None
    payment_reference: str | None = None
    payment_amount: int | None = None

class TournamentGenerateGroups(BaseModel):
    groups_count: int

class TournamentMatchResult(BaseModel):
    score: str
    winner_pair_id: int

class TournamentGeneratePlayoff(BaseModel):
    qualifiers_per_group: int = 2

class PlayerForgotPassword(BaseModel):
    email: str
class PlayerResetPassword(BaseModel):
    token: str
    new_password: str
