"""
Validation script using Pydantic for data quality checks.
Validates ingested data against defined models and business rules.
"""
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime

# VALIDATION MODELS - Define Pydantic models for each dataset to enforce schema and business rules
class Team(BaseModel):
    team_id: str
    name: str
    league: str
    stadium: str
    city: str

class Player(BaseModel):
    player_id: str
    name: str
    team_id: str
    position: str
    date_of_birth: Optional[str] = None
    nationality: Optional[str] = None
    market_value: Optional[float] = None
    contract_until: Optional[str] = None
    
    @field_validator('date_of_birth')
    def validate_birth_date_format(value):
        """Validate date_of_birth formats (YYYY-MM-DD or DD/MM/YYYY)."""
        formats = ["%Y-%m-%d", "%d/%m/%Y"]

        for format in formats:
            try:
                datetime.strptime(value, format)
                return value
            except ValueError:
                continue
        
        raise ValueError(f"Invalid date_of_birth format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY")

    @field_validator('contract_until')
    def validate_contract_until_date_format(value):
        """Validate contract_until date formats (YYYY-MM-DD or DD/MM/YYYY)."""
        formats = ["%Y-%m-%d", "%d/%m/%Y"]

        for format in formats:
            try:
                datetime.strptime(value, format)
                return value
            except ValueError:
                continue
        
        raise ValueError(f"Invalid contract_until format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY")
    
    @field_validator('market_value')
    def validate_market_value(value):
        """Market value should be positive."""
        if value is not None and value < 0:
            raise ValueError(f"Market value cannot be negative: {value}")
        return value

class Match(BaseModel):
    match_id: str
    competition: str
    season: str 
    match_date: str
    home_team_id: str
    away_team_id: str
    home_score: int = Field(ge=0)
    away_score: int = Field(ge=0)
    venue: str
    attendance: int = Field(ge=0)
    referee: str
    
    @field_validator('match_date')
    def validate_match_date(value):
        """Validate match_date format."""
        formats = ["%Y-%m-%d", "%d/%m/%Y"]
        for format in formats:
            try:
                datetime.strptime(value, format)
                return value
            except ValueError:
                continue
        raise ValueError(f"Invalid match_date format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY")

class PlayerMatchStats(BaseModel):
    player_id: str
    match_id: str
    minutes_played: int = Field(ge=0)
    goals: int = Field(ge=0)
    assists: int = Field(ge=0)
    shots: int = Field(ge=0)
    shots_on_target: int = Field(ge=0)
    passes_attempted: int = Field(ge=0)
    passes_completed: int = Field(ge=0)
    key_passes: int = Field(ge=0)
    tackles: int = Field(ge=0)
    interceptions: int = Field(ge=0)
    duels_won: int = Field(ge=0)
    duels_lost: int = Field(ge=0)
    fouls_committed: int = Field(ge=0)
    yellow_cards: int = Field(ge=0, le=2)
    red_cards: int = Field(ge=0, le=1)
    xg: float = Field(ge=0)
    xa: float = Field(ge=0)
    
    @model_validator(mode='after')
    def validate_shots_on_target(self):
        """Shots on target cannot be greater than total shots."""
        if self.shots_on_target > self.shots:
            raise ValueError(f"Shots on target ({self.shots_on_target}) cannot be greater than total shots ({self.shots})")
        return self
    
    @model_validator(mode='after')
    def validate_passes_completed(self):
        """Completed passes cannot be greater than attempted passes."""
        if self.passes_completed > self.passes_attempted:
            raise ValueError(f"Passes completed ({self.passes_completed}) cannot be greater than passes attempted ({self.passes_attempted})")
        return self
    
    @model_validator(mode='after')
    def validate_xg_logic(self):
        """Validate xG cannot be greater than total shots."""
        if self.xg > self.shots:
            raise ValueError(f"xG ({self.xg}) cannot be greater than total shots ({self.shots})")
        return self

class MatchEvent(BaseModel):
    event_id: str
    match_id: str
    minute: int = Field(ge=0)
    second: int = Field(ge=0, le=59)
    event_type: str
    player_id: str
    team_id: str
    x_start: float = Field(None, ge=0, le=100)
    y_start: float = Field(None, ge=0, le=100)
    x_end: Optional[float] = Field(None, ge=0, le=100)
    y_end: Optional[float] = Field(None, ge=0, le=100)
    outcome: str
    body_part: Optional[str] = None
    pass_type: Optional[str] = None
    recipient_id: Optional[str] = None
