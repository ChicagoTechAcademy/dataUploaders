-- SY27 roster x schedule x graduation credit
--
-- One row per student per course per graduation requirement that course
-- satisfies. A course satisfying three requirements produces three rows, so
-- COUNT(*) here is requirements, NOT courses and NOT students.
--
-- Joins on the course NUMBER, never the course name. Names are not unique in
-- the catalog -- 464 titles map to more than one course number.
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

studentCourses AS (
  -- The course number is everything before the first hyphen: 7 characters.
  -- SUBSTR(classCode, 1, 8) would include the hyphen and match nothing.
  SELECT
    s.id,
    s.classCode,
    SPLIT(s.classCode, '-')[OFFSET(0)] AS courseNumber,
    s.class      AS scheduledName,
    s.teacher,
    s.period,
    s.semester,
    s.entryID    AS scheduleEntryID
  FROM latestSchedules s
)

SELECT
  r.id,
  r.name,
  r.yog,
  r.homeroom,
  r.entryID          AS rosterEntryID,
  sc.scheduleEntryID,
  sc.courseNumber,
  c.title            AS courseTitle,
  sc.period,
  sc.semester,
  sc.teacher,
  c.academicLevel,
  c.creditsEarned,
  creditType         AS gradRequirement,
  priority + 1       AS requirementPriority,
  c.gradCreditRaw
FROM latestRoster r
LEFT JOIN studentCourses sc
  ON sc.id = r.id
LEFT JOIN academics.cpsCourseCatalog c
  ON c.courseNumber = sc.courseNumber
-- Credit types are stored in creditType1..creditType10 in DESCENDING PRIORITY:
-- a course fills the highest unmet requirement first, and only one at a time.
-- All ten are unpivoted and the nulls dropped; `priority` preserves the rank so
-- the fall-through order survives the unpivot.
-- LEFT JOIN UNNEST keeps courses that satisfy no requirement, and students who
-- have no schedule at all.
LEFT JOIN UNNEST([
  c.creditType1, c.creditType2, c.creditType3, c.creditType4, c.creditType5,
  c.creditType6, c.creditType7, c.creditType8, c.creditType9, c.creditType10
]) AS creditType WITH OFFSET AS priority
WHERE creditType IS NOT NULL OR sc.courseNumber IS NULL OR c.courseNumber IS NULL
ORDER BY r.name, sc.period, requirementPriority
