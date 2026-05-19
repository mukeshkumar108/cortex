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


class SessionHandoverPacketResponse(BaseModel):
    exists: bool = False
    tenantId: Optional[str] = None
    userId: Optional[str] = None
    sessionId: Optional[str] = None
    summary: Optional[str] = None
    openQuestions: List[str] = []
    unresolvedDecisions: List[str] = []
    pendingActions: List[str] = []
    recentStateNote: Optional[str] = None
    importantPeople: List[str] = []
    activeTopics: List[str] = []
    doNotOverdo: List[str] = []
    createdAt: Optional[str] = None
    expiresAt: Optional[str] = None
    sourceTurnRefs: List[Dict[str, Any]] = []
    status: Optional[str] = None
    metadata: Dict[str, Any] = {}


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


class ActionableCandidateItem(BaseModel):
    id: int
    kind: str
    candidateSubtype: Optional[str] = None
    title: str
    summary: Optional[str] = None
    priority: Optional[str] = None
    dueIso: Optional[str] = None
    startIso: Optional[str] = None
    endIso: Optional[str] = None
    waitingOn: Optional[str] = None
    needsResponse: Optional[bool] = None
    cadenceText: Optional[str] = None
    sourceType: str = "chat"
    sourceId: Optional[str] = None
    sourceUrl: Optional[str] = None
    collectedAt: Optional[str] = None
    updatedAt: Optional[str] = None
    provenance: Dict[str, Any] = {}
    confidence: Optional[float] = None
    confidenceLabel: Optional[str] = None
    userConfirmed: Optional[bool] = None
    status: str

    # Legacy compatibility fields; do not use for new consumers.
    suggestedAction: Optional[str] = None
    linkedExternalId: Optional[str] = None
    linkedExternalType: Optional[str] = None
    sessionId: Optional[str] = None
    recordType: Optional[str] = None
    relevantFromIso: Optional[str] = None
    relevantUntilIso: Optional[str] = None
    source: Optional[str] = None
    createdAt: Optional[str] = None


class DailyCandidatesResponse(BaseModel):
    tenantId: str
    userId: str
    asOf: str
    needsReview: List[ActionableCandidateItem] = []
    upcomingCommitments: List[ActionableCandidateItem] = []
    detected: List[ActionableCandidateItem] = []
    metadata: Dict[str, Any] = {}


class ReviewQueueResponse(BaseModel):
    tenantId: str
    userId: str
    asOf: str
    items: List[ActionableCandidateItem] = []
    byType: Dict[str, List[ActionableCandidateItem]] = {}
    metadata: Dict[str, Any] = {}


class AttentionPreviewItem(BaseModel):
    id: str
    tenant_id: str
    user_id: str
    companion_id: str
    title: str
    reason: str
    attention_type: str
    surface_mode: str
    action_policy: str
    priority: int
    urgency: int
    importance: int
    confidence: float
    sensitivity: str
    earliest_surface_at: Optional[str] = None
    ideal_surface_at: Optional[str] = None
    latest_useful_at: Optional[str] = None
    expires_at: Optional[str] = None
    status: str
    source_table: str
    source_id: str
    source_object_ids: List[str] = []
    source_link_ids: List[str] = []
    evidence_refs: List[Dict[str, Any]] = []
    gaps: List[str] = []
    missing_metadata: List[str] = []


class AttentionPreviewResponse(BaseModel):
    tenantId: str
    userId: str
    companionId: str
    asOf: str
    items: List[AttentionPreviewItem] = []
    metadata: Dict[str, Any] = {}


class SessionChangeItem(BaseModel):
    id: int
    kind: str
    title: str
    summary: Optional[str] = None
    effectiveIso: Optional[str] = None
    sourceType: str = "chat"
    sourceId: Optional[str] = None
    sourceUrl: Optional[str] = None
    collectedAt: Optional[str] = None
    updatedAt: Optional[str] = None
    provenance: Dict[str, Any] = {}
    confidence: Optional[float] = None
    confidenceLabel: Optional[str] = None
    status: str
    sessionId: Optional[str] = None


class SessionChangesResponse(BaseModel):
    tenantId: str
    userId: str
    asOf: str
    items: List[SessionChangeItem] = []
    byKind: Dict[str, List[SessionChangeItem]] = {}
    metadata: Dict[str, Any] = {}


class EntityCandidateItem(BaseModel):
    id: int
    name: str
    candidateType: str
    summary: Optional[str] = None
    sourceType: str = "chat"
    sourceId: Optional[str] = None
    sourceUrl: Optional[str] = None
    collectedAt: Optional[str] = None
    updatedAt: Optional[str] = None
    provenance: Dict[str, Any] = {}
    confidence: Optional[float] = None
    confidenceLabel: Optional[str] = None
    status: str
    sessionId: Optional[str] = None


class EntityCandidatesResponse(BaseModel):
    tenantId: str
    userId: str
    asOf: str
    items: List[EntityCandidateItem] = []
    byType: Dict[str, List[EntityCandidateItem]] = {}
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

class ActionItemCreateRequest(BaseModel):
    tenantId: str
    userId: str
    kind: str
    title: str
    notes: Optional[str] = None
    dueAt: Optional[datetime] = None
    remindAt: Optional[datetime] = None
    recurrenceRule: Optional[str] = None
    sourceType: Optional[str] = None
    sourceRef: Optional[Dict[str, Any]] = None
    confidence: Optional[float] = None
    provenanceSummary: Optional[str] = None


class ActionItemStatusUpdateRequest(BaseModel):
    tenantId: str
    userId: str
    status: str
    reason: Optional[str] = None


class ActionItemPatchRequest(BaseModel):
    tenantId: str
    userId: str
    actor: Optional[str] = "user"
    reason: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None
    kind: Optional[str] = None
    dueAt: Optional[datetime] = None
    due_at: Optional[datetime] = None
    remindAt: Optional[datetime] = None
    remind_at: Optional[datetime] = None
    recurrenceRule: Optional[str] = None
    status: Optional[str] = None


class CandidatePromotionRequest(BaseModel):
    tenantId: str
    userId: str
    candidateId: int
    overrides: Optional[Dict[str, Any]] = None


class DailyAgendaResponse(BaseModel):
    date: str
    timezone: str
    dueToday: List[Dict[str, Any]] = []
    remindersToday: List[Dict[str, Any]] = []
    overdue: List[Dict[str, Any]] = []
    pendingCandidates: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}
    suggestedActions: List[str] = []


class CalendarItemCreateRequest(BaseModel):
    tenantId: str
    userId: str
    title: str
    description: Optional[str] = None
    notes: Optional[str] = None
    startsAt: datetime
    endsAt: Optional[datetime] = None
    timezone: Optional[str] = "UTC"
    allDay: Optional[bool] = False
    location: Optional[str] = None
    participants: Optional[List[Dict[str, Any]]] = None
    organizer: Optional[Dict[str, Any]] = None
    rsvpStatus: Optional[str] = None
    status: Optional[str] = None
    sourceKind: Optional[str] = None
    sourceRef: Optional[Dict[str, Any]] = None
    evidenceRefs: Optional[List[Dict[str, Any]]] = None
    provenanceSummary: Optional[str] = None
    confidence: Optional[float] = None
    externalProvider: Optional[str] = None
    externalId: Optional[str] = None
    externalCalendarId: Optional[str] = None
    externalEtag: Optional[str] = None
    externalUpdatedAt: Optional[datetime] = None
    syncStatus: Optional[str] = None
    recurrenceRule: Optional[str] = None
    recurrenceParentId: Optional[str] = None
    dedupeKey: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class CalendarItemPatchRequest(BaseModel):
    tenantId: str
    userId: str
    actor: Optional[str] = "user"
    reason: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    notes: Optional[str] = None
    startsAt: Optional[datetime] = None
    endsAt: Optional[datetime] = None
    timezone: Optional[str] = None
    allDay: Optional[bool] = None
    location: Optional[str] = None
    participants: Optional[List[Dict[str, Any]]] = None
    organizer: Optional[Dict[str, Any]] = None
    rsvpStatus: Optional[str] = None
    status: Optional[str] = None
    sourceKind: Optional[str] = None
    sourceRef: Optional[Dict[str, Any]] = None
    evidenceRefs: Optional[List[Dict[str, Any]]] = None
    provenanceSummary: Optional[str] = None
    confidence: Optional[float] = None
    externalProvider: Optional[str] = None
    externalId: Optional[str] = None
    externalCalendarId: Optional[str] = None
    externalEtag: Optional[str] = None
    externalUpdatedAt: Optional[datetime] = None
    syncStatus: Optional[str] = None
    recurrenceRule: Optional[str] = None
    recurrenceParentId: Optional[str] = None
    dedupeKey: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class GoogleCalendarImportRequest(BaseModel):
    tenantId: str = "default"
    userId: str
    dateFrom: str
    dateTo: str
    googleEmail: Optional[str] = None
