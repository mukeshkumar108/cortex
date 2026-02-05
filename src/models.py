from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID


# Request Models
class IngestRequest(BaseModel):
    tenantId: str
    userId: str
    personaId: str
    role: str = Field(..., pattern="^(user|assistant)$")
    text: str
    timestamp: str
    sessionId: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class BriefRequest(BaseModel):
    tenantId: str
    userId: str
    personaId: str
    now: str
    sessionId: Optional[str] = None
    query: Optional[str] = None




# Response Models
class IngestResponse(BaseModel):
    status: str
    sessionId: Optional[str] = None
    identityUpdates: Optional[Dict[str, Any]] = None
    loopsDetected: Optional[List[UUID]] = None
    loopsCompleted: Optional[List[UUID]] = None
    graphitiAdded: bool = False


class Message(BaseModel):
    role: str
    text: str
    timestamp: str


class Fact(BaseModel):
    text: str
    relevance: Optional[float] = None
    source: Optional[str] = None


class Entity(BaseModel):
    summary: str
    type: Optional[str] = None
    uuid: Optional[str] = None


class Loop(BaseModel):
    id: UUID
    type: str
    status: Optional[str] = None
    text: str
    confidence: Optional[float] = None
    salience: Optional[int] = None
    timeHorizon: Optional[str] = None
    sourceTurnTs: Optional[str] = None
    dueDate: Optional[str] = None
    entityRefs: List[str] = []
    tags: List[str] = []
    createdAt: str
    updatedAt: Optional[str] = None
    lastSeenAt: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class NudgeCandidate(BaseModel):
    loopId: UUID
    type: str
    text: str
    question: str
    confidence: float
    evidenceText: Optional[str] = None


class TemporalAuthority(BaseModel):
    currentTime: str
    currentDay: str
    timeOfDay: str
    timeSinceLastInteraction: Optional[str] = None


class BriefResponse(BaseModel):
    # Tier 1: Guaranteed, fast (<100ms from Postgres)
    identity: Dict[str, Any]
    temporalAuthority: TemporalAuthority
    sessionState: Optional[Dict[str, Any]] = None
    workingMemory: List[Message] = []
    rollingSummary: Optional[str] = None  # Compressed history
    activeLoops: List[Loop] = []
    nudgeCandidates: List[NudgeCandidate] = []

    # Tier 2: Best-effort (Graphiti, 500ms timeout)
    episodeBridge: Optional[str] = None
    semanticContext: List[Fact] = []
    entities: List[Entity] = []

    # Meta
    instructions: List[str] = []
    metadata: Dict[str, Any] = {}


class MemoryQueryRequest(BaseModel):
    tenantId: str
    userId: str
    query: str
    limit: Optional[int] = 10
    referenceTime: Optional[str] = None


class MemoryQueryResponse(BaseModel):
    facts: List[Fact] = []
    entities: List[Entity] = []
    metadata: Dict[str, Any] = {}


class SessionCloseRequest(BaseModel):
    tenantId: str
    userId: str
    sessionId: Optional[str] = None
    personaId: Optional[str] = None
