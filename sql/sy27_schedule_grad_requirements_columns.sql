-- SY27 roster x schedule x graduation credit, ONE ROW PER STUDENT PER COURSE,
-- with each graduation requirement in its own column.
--
-- Companion to sy27_schedule_grad_requirements.sql, which explodes the same
-- data to one row per requirement. Use this one to read a schedule; use that
-- one to count or filter by requirement.
WITH latestRoster AS (
  -- roster is a snapshot table: every pull ever loaded is in it. Three SY27
  -- pulls exist as of 2026-09-01, so this must be pinned or each student
  -- appears once per pull.
  SELECT id, name, yog, homeroom, entryID
  FROM studentInfo.roster
  WHERE sy = 'SY27'
    AND entryID = (SELECT MAX(entryID) FROM studentInfo.roster WHERE sy = 'SY27')
),

latestSchedules AS (
  -- studentSchedules is loaded replace-all, so it holds one pull today and this
  -- filter is a no-op. It is here so the query stays correct if the load ever
  -- becomes append-only -- silently returning every historical pull is the kind
  -- of failure that reads as plausible.
  SELECT *
  FROM schedules.studentSchedules
  WHERE entryID = (SELECT MAX(entryID) FROM schedules.studentSchedules)
),

courseCredit AS (
  -- The catalog stores credit types across creditType1..10 in DESCENDING
  -- PRIORITY: a course fills the highest unmet requirement first, and only one
  -- at a time. gradReq1 is therefore the requirement this course fills FIRST,
  -- not an arbitrary pick. Nulls are dropped and the list re-packed from the
  -- left so the priority order survives even if the catalog ships a gap.
  SELECT
    courseNumber,
    title,
    academicLevel,
    creditsEarned,
    gradCreditRaw,
    ARRAY(
      SELECT ct FROM UNNEST([
        creditType1, creditType2, creditType3, creditType4, creditType5,
        creditType6, creditType7, creditType8, creditType9, creditType10
      ]) ct WHERE ct IS NOT NULL
    ) AS reqs
  FROM academics.cpsCourseCatalog
)

SELECT
  r.id,
  r.name,
  r.yog,
  r.homeroom,
  r.entryID                          AS rosterEntryID,
  s.entryID                          AS scheduleEntryID,
  SPLIT(s.classCode, '-')[OFFSET(0)] AS courseNumber,
  c.title                            AS courseTitle,
  s.period,
  s.semester,
  s.teacher,
  c.academicLevel,
  c.creditsEarned,

  -- gradReq1 is the requirement this course fills FIRST; gradReq2 is what it
  -- falls through to if gradReq1 is already met, and so on. Only ONE is ever
  -- filled by a given course.
  --
  -- Four columns covers every course currently scheduled: of the 47, one
  -- satisfies 1 requirement, 14 satisfy 2, 28 satisfy 3 and 4 satisfy 4.
  -- requirementCount is the guard -- if it ever exceeds 4, a requirement is
  -- being dropped off the end of this list and a gradReq5 is needed.
  c.reqs[SAFE_OFFSET(0)]             AS gradReq1,
  c.reqs[SAFE_OFFSET(1)]             AS gradReq2,
  c.reqs[SAFE_OFFSET(2)]             AS gradReq3,
  c.reqs[SAFE_OFFSET(3)]             AS gradReq4,
  ARRAY_LENGTH(c.reqs)               AS requirementCount,

  c.gradCreditRaw
FROM latestRoster r
LEFT JOIN latestSchedules s
  ON s.id = r.id
LEFT JOIN courseCredit c
  ON c.courseNumber = SPLIT(s.classCode, '-')[OFFSET(0)]
ORDER BY r.name, s.period
