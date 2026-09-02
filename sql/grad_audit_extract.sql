-- GRADUATION AUDIT EXTRACT
-- One row per (student, requirement column, semester) with a status and the
-- evidence behind it. Feeds the counsellor sheet; see [[Graduation Audit Intent]].
--
-- STATUS ALGORITHM (do not reorder -- CPS gives the coarse answer, the
-- transcript resolves which semester; going the other way walks into the
-- bucket-to-slot trap):
--
--   CPS says EARNED     -> enrolled now? yes -> "In Class, but Requirement Already Met"
--                                         no -> "Complete"
--   CPS says NOT earned -> transcript has this semester?  yes -> "Complete"
--                                                          no -> scheduled? yes -> "In-Progress"
--                                                                            no -> "Incomplete"
--
-- "Will Take in Future" is NOT computed here. It is a column-level dropdown in
-- the sheet that overrides whatever this returns.

WITH
-- ---------------------------------------------------------------------------
-- 1. The columns of the sheet.
--
-- matchMode decides how a transcript row is credited to a slot:
--   'semester'   -- the slot is a specific named course, so the transcript's
--                   own S1/S2 is used. A student who passed Biology S2 but
--                   failed S1 must not read as having S1.
--   'cumulative' -- the slot is interchangeable (any Fine Arts course counts),
--                   so slots fill in order against total credits earned. This
--                   says "you have 1.5 of 2 Fine Arts" without claiming which
--                   course filled which slot, which the data cannot support.
-- ---------------------------------------------------------------------------
slots AS (
  SELECT * FROM UNNEST([
    -- sheetColumn, cpsBucket, creditTypePattern, semester, matchMode, needCredits, sortOrder, repeatable, matchType
    --
    -- matchType is 'exact' everywhere except World Language. A prefix match is
    -- WRONG for most of these: 'English I' is a prefix of 'English II',
    -- 'English III' and 'English IV', and 'Physical Education I' is a prefix of
    -- 'Physical Education II'. Prefix matching flagged 320 students as already
    -- having English I because they were enrolled in English IV.
    --
    -- World Language genuinely needs a prefix: the catalog splits it into
    -- 'World Language', 'World Language French', 'World Lang Spanish' and a
    -- dozen more. 'World Lang' catches them all and does not touch
    -- 'World Studies'.
    STRUCT('Computer Science' AS sheetColumn, 'Computer Science' AS cpsBucket, 'Computer Science' AS creditTypePattern, 'S1' AS semester, 'semester' AS matchMode, 0.5 AS needCredits,  1 AS sortOrder, FALSE AS repeatable, 'exact' AS matchType),
    ('Computer Science',     'Computer Science',     'Computer Science',     'S2', 'semester',   1.0,  2, FALSE, 'exact'),
    ('English I',            'English I',            'English I',            'S1', 'semester',   0.5,  3, FALSE, 'exact'),
    ('English I',            'English I',            'English I',            'S2', 'semester',   1.0,  4, FALSE, 'exact'),
    ('English II',           'English II',           'English II',           'S1', 'semester',   0.5,  5, FALSE, 'exact'),
    ('English II',           'English II',           'English II',           'S2', 'semester',   1.0,  6, FALSE, 'exact'),
    ('English III',          'English III',          'English III',          'S1', 'semester',   0.5,  7, FALSE, 'exact'),
    ('English III',          'English III',          'English III',          'S2', 'semester',   1.0,  8, FALSE, 'exact'),
    ('English IV',           'English IV',           'English IV',           'S1', 'semester',   0.5,  9, FALSE, 'exact'),
    ('English IV',           'English IV',           'English IV',           'S2', 'semester',   1.0, 10, FALSE, 'exact'),
    ('Algebra',              'Algebra',              'Algebra',              'S1', 'semester',   0.5, 11, FALSE, 'exact'),
    ('Algebra',              'Algebra',              'Algebra',              'S2', 'semester',   1.0, 12, FALSE, 'exact'),
    ('Geometry',             'Geometry',             'Geometry',             'S1', 'semester',   0.5, 13, FALSE, 'exact'),
    ('Geometry',             'Geometry',             'Geometry',             'S2', 'semester',   1.0, 14, FALSE, 'exact'),
    ('Adv Alg/Trig',         'Adv Alg/Trig',         'Adv Alg/Trig',         'S1', 'semester',   0.5, 15, FALSE, 'exact'),
    ('Adv Alg/Trig',         'Adv Alg/Trig',         'Adv Alg/Trig',         'S2', 'semester',   1.0, 16, FALSE, 'exact'),
    ('Biology',              'Science',              'Biology',              'S1', 'semester',   0.5, 17, FALSE, 'exact'),
    ('Biology',              'Science',              'Biology',              'S2', 'semester',   1.0, 18, FALSE, 'exact'),
    ('Chemistry',            'Science',              'Chemistry',            'S1', 'semester',   0.5, 19, FALSE, 'exact'),
    ('Chemistry',            'Science',              'Chemistry',            'S2', 'semester',   1.0, 20, FALSE, 'exact'),
    ('Physics',              'Science',              'Physics',              'S1', 'semester',   0.5, 21, FALSE, 'exact'),
    ('Physics',              'Science',              'Physics',              'S2', 'semester',   1.0, 22, FALSE, 'exact'),
    ('Civics',               'Civics',               'Civics',               'S1', 'semester',   0.5, 23, FALSE, 'exact'),
    ('Other Social Science', 'Other Social Science', 'Other Social Science', 'S1', 'semester',   0.5, 24, FALSE, 'exact'),
    ('U.S. History',         'U.S. History',         'U.S. History',         'S1', 'semester',   0.5, 25, FALSE, 'exact'),
    ('U.S. History',         'U.S. History',         'U.S. History',         'S2', 'semester',   1.0, 26, FALSE, 'exact'),
    ('World Studies',        'World Studies',        'World Studies',        'S1', 'semester',   0.5, 27, FALSE, 'exact'),
    ('World Studies',        'World Studies',        'World Studies',        'S2', 'semester',   1.0, 28, FALSE, 'exact'),
    -- World Language I and II are interchangeable: any language course counts,
    -- and the transcript cannot say which course was meant for which slot.
    ('World Language I',     'World Lang I & II',    'World Lang',           'S1', 'cumulative', 0.5, 29, FALSE, 'prefix'),
    ('World Language I',     'World Lang I & II',    'World Lang',           'S2', 'cumulative', 1.0, 30, FALSE, 'prefix'),
    ('World Language II',    'World Lang I & II',    'World Lang',           'S1', 'cumulative', 1.5, 31, FALSE, 'prefix'),
    ('World Language II',    'World Lang I & II',    'World Lang',           'S2', 'cumulative', 2.0, 32, FALSE, 'prefix'),
    ('Fine Arts I',          'Fine Arts',            'Fine Arts',            'S1', 'cumulative', 0.5, 33, TRUE, 'exact'),
    ('Fine Arts I',          'Fine Arts',            'Fine Arts',            'S2', 'cumulative', 1.0, 34, TRUE, 'exact'),
    ('Fine Arts II',         'Fine Arts',            'Fine Arts',            'S1', 'cumulative', 1.5, 35, TRUE, 'exact'),
    ('Fine Arts II',         'Fine Arts',            'Fine Arts',            'S2', 'cumulative', 2.0, 36, TRUE, 'exact'),
    ('Physical Ed I',        'Physical Ed I',        'Physical Education I',  'S1', 'semester',   0.5, 37, FALSE, 'exact'),
    ('Physical Ed I',        'Physical Ed I',        'Physical Education I',  'S2', 'semester',   1.0, 38, FALSE, 'exact'),
    ('Physical Ed II',       'Physical Ed II',       'Physical Education II', 'S1', 'semester',   0.5, 39, FALSE, 'exact'),
    ('Physical Ed II',       'Physical Ed II',       'Physical Education II', 'S2', 'semester',   1.0, 40, FALSE, 'exact'),
    -- Grouped by request: "not overly concerned with Career Ed or Electives".
    ('Career Ed',            'Career Ed',            'Career Ed',            '',   'cumulative', 1.0, 41, FALSE, 'exact'),
    ('Elective',             'Elective',             'Elective',             '',   'cumulative', 3.0, 42, FALSE, 'exact')
  ])
),

-- ---------------------------------------------------------------------------
-- 1b. The non-credit requirement columns.
--
-- These carry no credits and no semester -- they are tasks, either done or not.
-- CPS's field names differ from what the sheet calls them, so the mapping is
-- explicit. Sheet labels must match the header row exactly or the lookup
-- returns a dash.
-- ---------------------------------------------------------------------------
nonCreditSlots AS (
  SELECT * FROM UNNEST([
    STRUCT('Driver Education Classroom'       AS sheetColumn, 'Drivers Ed'                      AS cpsRequirement, 43 AS sortOrder),
    ('Public Law 195 Test',                         'Constitution Exam',                44),
    ('Financial Education',                         'Consumer/Financial Education',     45),
    ('Health Education',                            'Health Education',                 46),
    ('SAT Assessment',                              'State Mandated Exam',              47),
    ('Civics Service Learning Project',             'Civics Service Learning Project',  48),
    ('Service Learning Project - Other',            'Service Learning Project 2',       49),
    ('Learn, Plan, Succeed',                        'Learn.Plan.Succeed.',              50),
    ('FAFSA',                                       'Financial Aid Application',        51)
  ])
),

-- ---------------------------------------------------------------------------
-- 2. The cohort. roster is a snapshot -- pin it or every student repeats once
--    per pull.
-- ---------------------------------------------------------------------------
students AS (
  SELECT id, name, yog, homeroom
  FROM studentInfo.roster
  WHERE sy = 'SY27'
    AND entryID = (SELECT MAX(entryID) FROM studentInfo.roster WHERE sy = 'SY27')
),

-- ---------------------------------------------------------------------------
-- 3. What CPS says about each bucket.
-- ---------------------------------------------------------------------------
cps AS (
  SELECT id, requirement AS cpsBucket, status AS cpsStatus,
         creditsEarned AS cpsCreditsEarned, creditsRequired AS cpsCreditsRequired
  FROM academics.gradReqProgress
  WHERE requirementKind = 'credit'
),

-- ---------------------------------------------------------------------------
-- 4. Transcript rows exploded to one row per (student, creditType, semester).
--    Only PASSED semesters count: an F earns 0 credits and must not satisfy
--    anything. The " Content 1/2" suffix is stripped -- it duplicates the
--    semester, which semesterCredit already carries (verified: Content 1 is
--    always S1, Content 2 always S2, no exceptions).
-- ---------------------------------------------------------------------------
transcript AS (
  SELECT
    t.id,
    REGEXP_REPLACE(ct, r' Content [12]$', '') AS creditType,
    t.semesterCredit,
    t.creditEarned,
    t.course,
    t.finalGrade,
    t.yearTaken
  FROM academics.transcriptAudit t,
  UNNEST([t.creditType1, t.creditType2, t.creditType3, t.creditType4]) ct
  WHERE ct IS NOT NULL AND t.creditEarned > 0
),

-- ---------------------------------------------------------------------------
-- 5. What each student is enrolled in NOW, by requirement.
-- ---------------------------------------------------------------------------
scheduled AS (
  SELECT DISTINCT
    s.id,
    REGEXP_REPLACE(ct, r' Content [12]$', '') AS creditType,
    s.class AS scheduledCourse,
    SPLIT(s.classCode, '-')[OFFSET(0)] AS scheduledCourseNumber
  FROM schedules.studentSchedules s
  JOIN academics.cpsCourseCatalog c
    ON c.courseNumber = SPLIT(s.classCode, '-')[OFFSET(0)]
  CROSS JOIN UNNEST([
    c.creditType1, c.creditType2, c.creditType3, c.creditType4, c.creditType5,
    c.creditType6, c.creditType7, c.creditType8, c.creditType9, c.creditType10
  ]) ct
  WHERE ct IS NOT NULL
    AND s.entryID = (SELECT MAX(entryID) FROM schedules.studentSchedules)
),

-- ---------------------------------------------------------------------------
-- 5b. Does a scheduled course also serve something the student still needs?
--
-- A course listing two requirements is not misplaced just because ONE of them
-- is already met. AS1 AP English Language and Composition carries both English
-- III and English IV: a senior who passed AP English as a junior has English III
-- met, but is plainly taking it for English IV, which they still need. Flagging
-- English III there is a false positive, and 19 students hit it.
--
-- Career Ed and Elective are deliberately EXCLUDED from what counts as "still
-- needed". Nearly every course in the catalog carries them, so allowing them to
-- rescue a course would suppress almost every flag -- including the Fine Arts
-- ones that are wanted. "It still counts for English IV" is a reason to hide a
-- flag; "it still counts for Elective" is not.
-- ---------------------------------------------------------------------------
courseServesUnmet AS (
  SELECT DISTINCT sc.id, sc.scheduledCourseNumber
  FROM scheduled sc
  JOIN cps c2
    ON c2.id = sc.id
   AND c2.cpsBucket = sc.creditType
  WHERE c2.cpsStatus != 'Yes'
    AND sc.creditType NOT IN ('Career Ed', 'Elective')
),

-- ---------------------------------------------------------------------------
-- 6. Every student against every column.
-- ---------------------------------------------------------------------------
grid AS (
  SELECT st.id, st.name, st.yog, st.homeroom, sl.*
  FROM students st CROSS JOIN slots sl
),

evidence AS (
  SELECT
    g.*,
    c.cpsStatus,
    c.cpsCreditsEarned,
    c.cpsCreditsRequired,

    -- semester mode: did THIS semester of THIS course pass?
    --
    -- Gated on CPS having credited the bucket at all. THE TRANSCRIPT SAYS WHICH,
    -- NEVER WHETHER -- without this gate it can invent credit CPS did not give.
    --
    -- The case that forced this: AP English carries BOTH "English III Content 1"
    -- and "English IV Content 1", because the course satisfies either depending
    -- on the year it is taken. CPS resolves that correctly (grade 11 -> English
    -- III) and reports English IV as 0.0 earned. Exploding every creditType let
    -- one AP English row mark English IV complete as well, so juniors read as
    -- having finished a senior course.
    (c.cpsCreditsEarned > 0
     AND (SELECT COUNT(*) FROM transcript t
           WHERE t.id = g.id
             AND (CASE WHEN g.matchType = 'prefix' THEN STARTS_WITH(t.creditType, g.creditTypePattern) ELSE t.creditType = g.creditTypePattern END)
             AND t.semesterCredit = g.semester) > 0)      AS semesterPassed,

    -- cumulative mode: enough total credits to reach this slot?
    -- Capped at what CPS credited, for the same reason as above.
    LEAST(IFNULL(c.cpsCreditsEarned, 0),
      IFNULL((SELECT SUM(t.creditEarned) FROM transcript t
             WHERE t.id = g.id
               AND (CASE WHEN g.matchType = 'prefix' THEN STARTS_WITH(t.creditType, g.creditTypePattern) ELSE t.creditType = g.creditTypePattern END)), 0))
                                                          AS cumulativeCredits,

    (SELECT STRING_AGG(DISTINCT t.course, '; ' ORDER BY t.course) FROM transcript t
      WHERE t.id = g.id AND (CASE WHEN g.matchType = 'prefix' THEN STARTS_WITH(t.creditType, g.creditTypePattern) ELSE t.creditType = g.creditTypePattern END)
        AND (g.matchMode = 'cumulative' OR t.semesterCredit = g.semester))
                                                          AS transcriptCourse,
    (SELECT STRING_AGG(DISTINCT t.finalGrade, '; ') FROM transcript t
      WHERE t.id = g.id AND (CASE WHEN g.matchType = 'prefix' THEN STARTS_WITH(t.creditType, g.creditTypePattern) ELSE t.creditType = g.creditTypePattern END)
        AND (g.matchMode = 'cumulative' OR t.semesterCredit = g.semester))
                                                          AS transcriptGrade,
    (SELECT STRING_AGG(DISTINCT sc.scheduledCourse, '; ') FROM scheduled sc
      WHERE sc.id = g.id AND (CASE WHEN g.matchType = 'prefix' THEN STARTS_WITH(sc.creditType, g.creditTypePattern) ELSE sc.creditType = g.creditTypePattern END))
                                                          AS scheduledCourse,

    -- TRUE when every scheduled course touching this requirement is also
    -- serving something the student still needs, so the flag is suppressed.
    (SELECT LOGICAL_AND(u.scheduledCourseNumber IS NOT NULL)
       FROM scheduled sc
       LEFT JOIN courseServesUnmet u
         ON u.id = sc.id AND u.scheduledCourseNumber = sc.scheduledCourseNumber
      WHERE sc.id = g.id
        AND (CASE WHEN g.matchType = 'prefix' THEN STARTS_WITH(sc.creditType, g.creditTypePattern) ELSE sc.creditType = g.creditTypePattern END))
                                                          AS scheduledServesUnmet
  FROM grid g
  LEFT JOIN cps c ON c.id = g.id AND c.cpsBucket = g.cpsBucket
)

SELECT
  id, name, yog, homeroom,
  sheetColumn,
  semester,
  CONCAT(sheetColumn, IF(semester = '', ' - ', CONCAT(' - ', semester))) AS columnLabel,
  sortOrder,

  CASE
    WHEN cpsStatus = 'Yes' AND scheduledCourse IS NOT NULL
         AND NOT IFNULL(scheduledServesUnmet, FALSE)
      THEN 'In Class, but Requirement Already Met'
    WHEN cpsStatus = 'Yes'
      THEN 'Complete'
    WHEN matchMode = 'semester'   AND semesterPassed          THEN 'Complete'
    WHEN matchMode = 'cumulative' AND cumulativeCredits >= needCredits THEN 'Complete'
    WHEN scheduledCourse IS NOT NULL THEN 'In-Progress'
    ELSE 'Incomplete'
  END AS status,

  -- evidence
  cpsStatus            AS cpsSaysEarned,
  cpsCreditsEarned,
  cpsCreditsRequired,
  matchMode,
  needCredits,
  cumulativeCredits,
  semesterPassed,
  transcriptCourse,
  transcriptGrade,
  scheduledCourse,
  scheduledServesUnmet,
  repeatable,
  cpsBucket
FROM evidence

UNION ALL

-- Non-credit requirements. CPS is the only source; there is no transcript or
-- schedule dimension to these, so the evidence columns are null by design.
-- "Waived" counts as Complete -- the requirement is satisfied, just not by
-- doing the thing.
SELECT
  st.id, st.name, st.yog, st.homeroom,
  n.sheetColumn,
  '' AS semester,
  n.sheetColumn AS columnLabel,
  n.sortOrder,
  CASE
    WHEN p.status IN ('Yes', 'Waived') THEN 'Complete'
    WHEN p.status = 'No'               THEN 'Incomplete'
    ELSE '-'
  END AS status,
  p.status                AS cpsSaysEarned,
  CAST(NULL AS FLOAT64)   AS cpsCreditsEarned,
  CAST(NULL AS FLOAT64)   AS cpsCreditsRequired,
  'nonCredit'             AS matchMode,
  CAST(NULL AS FLOAT64)   AS needCredits,
  CAST(NULL AS FLOAT64)   AS cumulativeCredits,
  CAST(NULL AS BOOL)      AS semesterPassed,
  CAST(NULL AS STRING)    AS transcriptCourse,
  CAST(NULL AS STRING)    AS transcriptGrade,
  CAST(NULL AS STRING)    AS scheduledCourse,
  CAST(NULL AS BOOL)      AS scheduledServesUnmet,
  FALSE                   AS repeatable,
  n.cpsRequirement        AS cpsBucket
FROM students st
CROSS JOIN nonCreditSlots n
LEFT JOIN academics.gradReqProgress p
  ON p.id = st.id AND p.requirement = n.cpsRequirement AND p.requirementKind = 'nonCredit'

ORDER BY name, sortOrder
