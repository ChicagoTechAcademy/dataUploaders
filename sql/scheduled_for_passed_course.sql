-- Students currently scheduled for a course they have ALREADY PASSED IN FULL.
--
-- "In full" means both semesters. Students legitimately retake a course when
-- they failed one semester and did not make it up in summer school, so passing
-- only S1 and being scheduled again is normal and must not appear here.
--
-- The semester is the SIXTH character of the course number: 184301R is RS1
-- English III, 184302R is RS2. Stripping that character gives the base course,
-- which is what a repeat is judged against.
--
-- EXCEPTION: Civics and Other Social Science require only 0.5 credits, so ONE
-- semester satisfies them and a single pass is already a full pass. Courses
-- carrying those credit types need one semester, not two. Everything else needs
-- both.
--
-- Passing means an A, B, C or D with credit earned. An F earns 0 credits; a P
-- is a 0-credit course; and 121 transcript rows earn credit with no recorded
-- grade, which is not solid enough to put in front of a counsellor as "they
-- already passed this".
WITH currentSchedule AS (
  SELECT
    s.id,
    SPLIT(s.classCode, '-')[OFFSET(0)] AS courseNumber,
    CONCAT(SUBSTR(SPLIT(s.classCode, '-')[OFFSET(0)], 1, 5),
           SUBSTR(SPLIT(s.classCode, '-')[OFFSET(0)], 7)) AS baseCourse,
    s.class   AS scheduledCourse,
    s.teacher,
    s.period
  FROM schedules.studentSchedules s
  WHERE s.entryID = (SELECT MAX(entryID) FROM schedules.studentSchedules)
),

-- Base courses where one semester is the whole requirement. Derived from
-- gradReqDefinition rather than hardcoded, so adding a 0.5-credit requirement
-- later needs no change here.
halfCreditCourses AS (
  SELECT DISTINCT CONCAT(SUBSTR(c.courseNumber, 1, 5), SUBSTR(c.courseNumber, 7)) AS baseCourse
  FROM academics.cpsCourseCatalog c,
  UNNEST([c.creditType1, c.creditType2, c.creditType3, c.creditType4]) ct
  WHERE ct IN (SELECT DISTINCT cpsBucket FROM academics.gradReqDefinition
               WHERE creditsRequired < 1)
),

passedSemesters AS (
  SELECT
    id,
    CONCAT(SUBSTR(courseNumber, 1, 5), SUBSTR(courseNumber, 7)) AS baseCourse,
    -- the semester digit, taken from the course number rather than
    -- semesterCredit: it is the thing that distinguishes the two catalog
    -- entries, and it is never null.
    SUBSTR(courseNumber, 6, 1) AS semesterDigit,
    finalGrade,
    yearTaken,
    creditEarned
  FROM academics.transcriptAudit
  WHERE creditEarned > 0
    AND finalGrade IN ('A', 'B', 'C', 'D')
),

passedBoth AS (
  SELECT
    id,
    baseCourse,
    STRING_AGG(DISTINCT CONCAT(CAST(yearTaken AS STRING), ' S', semesterDigit,
                               ' (', finalGrade, ')')) AS passedWhen,
    SUM(creditEarned) AS creditEarnedPreviously
  FROM passedSemesters
  GROUP BY id, baseCourse
  -- Both halves, except where one semester IS the requirement.
  HAVING COUNT(DISTINCT semesterDigit) >=
    IF(baseCourse IN (SELECT baseCourse FROM halfCreditCourses), 1, 2)
)

SELECT
  r.name,
  c.id,
  r.yog,
  r.homeroom,
  c.scheduledCourse,
  c.courseNumber,
  c.period,
  c.teacher,
  p.passedWhen,
  p.creditEarnedPreviously,
  c.baseCourse IN (SELECT baseCourse FROM halfCreditCourses) AS oneSemesterRequirement
FROM currentSchedule c
JOIN passedBoth p
  ON p.id = c.id AND p.baseCourse = c.baseCourse
JOIN (
  SELECT id, name, yog, homeroom
  FROM studentInfo.roster
  WHERE sy = 'SY27'
    AND entryID = (SELECT MAX(entryID) FROM studentInfo.roster WHERE sy = 'SY27')
) r ON r.id = c.id
ORDER BY r.name, c.period
