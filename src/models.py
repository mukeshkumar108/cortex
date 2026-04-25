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
    relevance_tier: Optional[str] = None
    domain: Optional[str] = None
    intent: Optional[str] = None
    memoryType: Optional[str] = None
    domainScores: Optional[Dict[str, float]] = None
    confidence: Optional[float] = None
    classificationMethod: Optional[str] = None
    sourceTenant: Optional[str] = None


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
    memoryIntent: Optional[str] = Field(default="exact", pattern="^(exact|episodic|hybrid)$")
    referenceTime: Optional[str] = None
    includeContext: Optional[bool] = False
    focusQuery: Optional[str] = None


class MemoryQueryV2Request(BaseModel):
    tenantId: str
    userId: str
    query: str
    lane: Optional[str] = Field(default="hybrid", pattern="^(factual|episodic|continuity|hybrid)$")
    limit: Optional[int] = 10
    referenceTime: Optional[str] = None


class MemoryQueryV2Item(BaseModel):
    lane: str
    itemType: str
    text: Optional[str] = None
    relevance: Optional[float] = None
    source: Optional[str] = None
    sourceTenant: Optional[str] = None
    derived: Optional[bool] = None
    dataClassification: Optional[str] = None
    claimSlotKey: Optional[str] = None
    claimEventKey: Optional[str] = None
    lifecycleStatus: Optional[str] = None
    evidence: List[Dict[str, Any]] = []
    episodeId: Optional[str] = None
    sessionId: Optional[str] = None
    referenceTime: Optional[str] = None
    linkedEntities: List[str] = []
    metadata: Dict[str, Any] = {}


class MemoryQueryV2Response(BaseModel):
    lane: str
    items: List[MemoryQueryV2Item] = []
    metadata: Dict[str, Any] = {}


class EpisodeRecallItem(BaseModel):
    episodeId: Optional[str] = None
    sessionId: Optional[str] = None
    referenceTime: Optional[str] = None
    score: Optional[float] = None
    summary: Optional[str] = None
    evidence: List[str] = []
    linkedEntities: List[str] = []
    sourceTenant: Optional[str] = None


class MemoryQueryResponse(BaseModel):
    facts: List[str] = []
    factItems: List[Fact] = []
    entities: List[Entity] = []
    episodes: List[EpisodeRecallItem] = []
    openLoops: Optional[List[str]] = None
    commitments: Optional[List[str]] = None
    contextAnchors: Optional[Dict[str, Any]] = None
    userStatedState: Optional[str] = None
    currentFocus: Optional[str] = None
    recallSheet: Optional[str] = None
    supplementalContext: Optional[str] = None
    metadata: Dict[str, Any] = {}


class MemoryLoopItem(BaseModel):
    id: str
    type: str
    text: str
    status: Optional[str] = None
    salience: Optional[int] = None
    timeHorizon: Optional[str] = None
    dueDate: Optional[str] = None
    lastSeenAt: Optional[str] = None
    domain: Optional[str] = None
    importance: Optional[int] = None
    urgency: Optional[int] = None
    tags: List[str] = []
    personaId: Optional[str] = None


class MemoryLoopsResponse(BaseModel):
    items: List[MemoryLoopItem] = []
    metadata: Dict[str, Any] = {}


class SessionBriefResponse(BaseModel):
    timeGapDescription: Optional[str] = None
    timeOfDayLabel: Optional[str] = None
    energyHint: Optional[str] = None
    facts: List[str] = []
    openLoops: List[str] = []
    commitments: List[str] = []
    contextAnchors: Dict[str, Any] = {}
    userStatedState: Optional[str] = None
    currentFocus: Optional[str] = None
    temporalVibe: Optional[str] = None
    briefContext: Optional[str] = None
    narrativeSummary: List[Dict[str, Any]] = []
    activeLoops: List[Dict[str, Any]] = []
    currentVibe: Dict[str, Any] = {}


class SessionStartBriefItem(BaseModel):
    kind: str
    text: str
    type: Optional[str] = None
    timeHorizon: Optional[str] = None
    dueDate: Optional[str] = None
    salience: Optional[int] = None
    lastSeenAt: Optional[str] = None


class SessionStartBriefEntityProfile(BaseModel):
    name: str
    profile_text: str
    facts: List[str] = []


class SessionStartBriefEntityHint(BaseModel):
    entityId: Optional[str] = None
    name: str
    type: str = "other"
    role: Optional[str] = None
    importance: Optional[str] = None
    salience: Optional[float] = None
    lastSeenAt: Optional[str] = None
    source: Optional[str] = None
    confidence: Optional[float] = None
    updatedAt: Optional[str] = None


class SessionStartBriefResponse(BaseModel):
    handover_text: str
    narrative: Optional[str] = None
    handover_depth: str
    time_context: Dict[str, Any] = {}
    resume: Dict[str, Any] = {}
    ops_context: Dict[str, Any] = {}
    evidence: Dict[str, Any] = {}
    entity_hints: List[SessionStartBriefEntityHint] = []
    # Legacy surface; kept temporarily for compatibility.
    entity_profiles: List[SessionStartBriefEntityProfile] = []


class AlwaysOnMemoryPacketResponse(BaseModel):
    version: str
    generated_at: str
    source_fingerprint: str
    profile_truth_used: bool = False
    sections: Dict[str, List[str]] = {}
    packet_text: str
    metadata: Dict[str, Any] = {}


class DeclaredProfilePerson(BaseModel):
    name: str
    relationship: Optional[str] = None
    note: Optional[str] = None
    situation: Optional[str] = None
    contact: Optional[str] = None
    context: Optional[str] = None
    directive: Optional[str] = None
    location: Optional[str] = None
    faith: Optional[str] = None


class DeclaredProfileTruth(BaseModel):
    preferred_name: Optional[str] = None
    location: Optional[str] = None
    timezone: Optional[str] = None
    age: Optional[str] = None
    faith: Optional[str] = None
    roles: List[str] = []
    projects: List[str] = []
    important_people: List[DeclaredProfilePerson] = []
    writing_or_public_work: List[str] = []
    health_considerations: List[str] = []
    notes_for_sophie: Optional[str] = None
    user_about: Optional[str] = None


class DeclaredProfileTruthPatchRequest(BaseModel):
    tenantId: str
    userId: str
    profile: DeclaredProfileTruth
    sourceSurface: Optional[str] = None
    updatedBy: Optional[str] = None
    reason: Optional[str] = None
    replace: bool = False


class DeclaredProfileTruthResponse(BaseModel):
    tenantId: str
    userId: str
    profile: DeclaredProfileTruth
    exists: bool = False
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    metadata: Dict[str, Any] = {}


class DeclaredProfileTruthHistoryItem(BaseModel):
    id: int
    sourceSurface: Optional[str] = None
    updatedBy: Optional[str] = None
    reason: Optional[str] = None
    changeSummary: Dict[str, List[str]] = {}
    createdAt: str


class DeclaredProfileTruthHistoryResponse(BaseModel):
    tenantId: str
    userId: str
    items: List[DeclaredProfileTruthHistoryItem] = []
    metadata: Dict[str, Any] = {}


class EntityProfileRequest(BaseModel):
    tenantId: str
    userId: str
    entityId: Optional[str] = None
    name: Optional[str] = None
    referenceTime: Optional[str] = None
    includeOpenLoops: Optional[bool] = True
    factsLimit: Optional[int] = 6
    loopsLimit: Optional[int] = 3


class EntityProfileResponse(BaseModel):
    entity: Dict[str, Any]
    keyFacts: List[Dict[str, Any]] = []
    openLoops: List[Dict[str, Any]] = []
    provenance: Dict[str, Any] = {}


class SessionCloseRequest(BaseModel):
    tenantId: str
    userId: str
    sessionId: Optional[str] = None
    personaId: Optional[str] = None


class SessionIngestRequest(BaseModel):
    tenantId: str
    userId: str
    personaId: Optional[str] = None
    sessionId: str
    startedAt: Optional[str] = None
    endedAt: Optional[str] = None
    messages: List[Message]


class SessionIngestResponse(BaseModel):
    status: str
    sessionId: str
    graphitiAdded: bool = False


class PurgeUserRequest(BaseModel):
    tenantId: str
    userId: str


class UserModelPatchRequest(BaseModel):
    tenantId: str
    userId: str
    patch: Dict[str, Any]
    source: Optional[str] = None


class UserModelResponse(BaseModel):
    tenantId: str
    userId: str
    model: Dict[str, Any]
    completenessScore: Dict[str, int] = {}
    metadata: Dict[str, Any] = {}
    version: int = 0
    exists: bool = False
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    lastSource: Optional[str] = None


class DailyAnalysisResponse(BaseModel):
    tenantId: str
    userId: str
    analysisDate: Optional[str] = None
    themes: List[str] = []
    scores: Dict[str, int] = {}
    steeringNote: Optional[str] = None
    confidence: Optional[float] = None
    exists: bool = False
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    metadata: Dict[str, Any] = {}


class HabitDailyLogUpsertRequest(BaseModel):
    tenantId: str
    userId: str
    habitId: str
    completed: Optional[bool] = None
    nudged: Optional[bool] = None
    userResponse: Optional[str] = None
    inferredFrom: Optional[str] = None


class HabitDailyLogUpsertResponse(BaseModel):
    status: str
    userId: str
    habitId: str
    date: str
    completed: bool
    nudged: bool
    userResponse: Optional[str] = None
    inferredFrom: Optional[str] = None


class DerivedSignal(BaseModel):
    key: str
    value: Optional[float] = None
    label: Optional[str] = None
    confidence: Optional[float] = None
    evidence: List[str] = []


class DerivedUserModel(BaseModel):
    schemaVersion: str = "v0.1"
    generatedAt: str
    userId: str
    tenantId: Optional[str] = None
    focusDomains: List[str] = []
    dominantIntents: List[str] = []
    activeSignals: List[DerivedSignal] = []
    confidence: float = 0.0
    provenance: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}


class RuntimeSteeringPacket(BaseModel):
    schemaVersion: str = "v0.1"
    generatedAt: str
    userId: str
    tenantId: Optional[str] = None
    query: Optional[str] = None
    queryDomain: Optional[str] = None
    queryIntent: Optional[str] = None
    queryMemoryType: Optional[str] = None
    queryDomainFocus: List[str] = []
    retrievalConfidence: Optional[float] = None
    riskFlags: List[str] = []
    steeringHints: List[str] = []
    constraints: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}
