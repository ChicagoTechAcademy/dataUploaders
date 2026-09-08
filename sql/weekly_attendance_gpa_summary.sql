-- Weekly staff snapshot: average attendance and average GPA by grade level,
-- plus a school total. Runnable at any time -- no week to set.
--
-- Each source is taken at ITS OWN most recent pull. In practice they are pulled
-- the same day; when they are not, the ranges can differ slightly and nothing
-- in the output says so -- the pull dates were dropped by request.
WITH
-- The roster to report against. Everything is filtered to these ids, so a
-- student who has left but is still generating attendance rows is excluded.
RosterFiltered AS (
  SELECT id, yog
  FROM `chitechdb`.`studentInfo`.`roster`
  WHERE entryID = (SELECT MAX(entryID) FROM `chitechdb`.`studentInfo`.`roster`)
),

-- Seniors are the earliest graduating class on the roster, so grade level is
-- derived rather than hardcoded and needs no edit next year.
GradeLevels AS (
  SELECT yog, 12 - (yog - MIN(yog) OVER ()) AS gradeLevel
  FROM (SELECT DISTINCT yog FROM RosterFiltered)
),

WeeklyAtt AS (
  SELECT id, daysPresent, daysEnrolled
  FROM `chitechdb`.`attendance`.`weeklyAttendance`
  WHERE entryID = (SELECT MAX(entryID) FROM `chitechdb`.`attendance`.`weeklyAttendance`)
),

TotalAtt AS (
  SELECT id, daysPresent, daysEnrolled
  FROM `chitechdb`.`attendance`.`totalAttendance`
  WHERE entryID = (SELECT MAX(entryID) FROM `chitechdb`.`attendance`.`totalAttendance`)
),

-- Every grade from the latest pull, one row each. NOT pre-averaged per student:
-- the GPA below pools all grades, so a student taking eight courses carries more
-- weight than one taking two.
--
-- Only A-F are kept. A grade outside that set is dropped from the numerator AND
-- the denominator, rather than scoring 0 while still being counted -- which is
-- what an ELSE 0 does, and it drags the average down invisibly.
GradePoints AS (
  SELECT
    g.id,
    CASE g.letterGrade
      WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2
      WHEN 'D' THEN 1 WHEN 'F' THEN 0
    END AS points
  FROM `chitechdb`.`academics`.`grades` g
  WHERE g.entryID = (SELECT MAX(entryID) FROM `chitechdb`.`academics`.`grades`)
    AND g.letterGrade IN ('A', 'B', 'C', 'D', 'F')
),

-- Attendance is one row per student per source; grades are many rows per
-- student. They are aggregated separately and stitched together by cohort so
-- the attendance figures are not multiplied by a student's course count.
-- Gradeable courses on the current schedule -- the denominator for "how much
-- has actually been entered".
--
-- Student Meal, Advisory and EMBARC are excluded: they appear on schedules but
-- no teacher enters a letter grade for them, so counting them would make the
-- gap look worse than it is and blunt the point of the column.
GradeableSchedule AS (
  SELECT s.id, COUNT(*) AS scheduledCourses
  FROM `chitechdb`.`schedules`.`studentSchedules` s
  WHERE s.entryID = (SELECT MAX(entryID) FROM `chitechdb`.`schedules`.`studentSchedules`)
    AND s.class NOT LIKE '%Student Meal%'
    AND s.class NOT LIKE '%Advisory%'
    AND s.class NOT LIKE '%EMBARC%'
  GROUP BY s.id
),

AttByGrade AS (
  SELECT
    gl.gradeLevel,
    ANY_VALUE(rf.yog)          AS yog,
    SUM(w.daysPresent)         AS wk_present,
    SUM(w.daysEnrolled)        AS wk_enrolled,
    SUM(t.daysPresent)         AS ytd_present,
    SUM(t.daysEnrolled)        AS ytd_enrolled,
    COUNT(DISTINCT rf.id)      AS students
  FROM RosterFiltered rf
  JOIN GradeLevels gl ON gl.yog = rf.yog
  LEFT JOIN WeeklyAtt w ON w.id = rf.id
  LEFT JOIN TotalAtt  t ON t.id = rf.id
  GROUP BY GROUPING SETS ((gl.gradeLevel), ())
),

SchedByGrade AS (
  SELECT
    gl.gradeLevel,
    SUM(sc.scheduledCourses) AS scheduledCourses
  FROM RosterFiltered rf
  JOIN GradeLevels gl        ON gl.yog = rf.yog
  JOIN GradeableSchedule sc  ON sc.id = rf.id
  GROUP BY GROUPING SETS ((gl.gradeLevel), ())
),

GpaByGrade AS (
  SELECT
    gl.gradeLevel,
    SUM(gp.points)              AS totalPoints,
    COUNT(gp.points)            AS gradeCount,
    COUNT(DISTINCT gp.id)       AS studentsWithGrades
  FROM RosterFiltered rf
  JOIN GradeLevels gl ON gl.yog = rf.yog
  JOIN GradePoints gp ON gp.id = rf.id
  GROUP BY GROUPING SETS ((gl.gradeLevel), ())
)

SELECT
  IF(a.gradeLevel IS NULL, 'Total', FORMAT('%d', a.gradeLevel)) AS grade,
  IF(a.gradeLevel IS NULL, 'Total', FORMAT('%d', a.yog))        AS yog,
  a.students,

  FORMAT('%0.2f%%', SAFE_DIVIDE(a.wk_present,  a.wk_enrolled)  * 100) AS weeklyAttendance,
  FORMAT('%0.2f%%', SAFE_DIVIDE(a.ytd_present, a.ytd_enrolled) * 100) AS ytdAttendance,

  -- Pooled: every grade counts once, so a student's weight is their course load.
  FORMAT('%0.3f', SAFE_DIVIDE(g.totalPoints, g.gradeCount)) AS avgGPA,

  -- Divided by students WITH grades, not by the whole cohort: a student with
  -- nothing entered yet would otherwise pull this down and read as a light
  -- course load rather than as missing data.
  FORMAT('%0.1f', SAFE_DIVIDE(g.gradeCount, g.studentsWithGrades))
    AS avgCoursesWithGrades,

  -- Grades entered against gradeable courses scheduled. This is the nudge: it
  -- says how much of the term's marking is actually done.
  FORMAT('%0.1f%%', SAFE_DIVIDE(g.gradeCount, sc.scheduledCourses) * 100)
    AS pctCoursesGraded
FROM AttByGrade a
LEFT JOIN GpaByGrade g
  ON g.gradeLevel = a.gradeLevel
  OR (g.gradeLevel IS NULL AND a.gradeLevel IS NULL)
LEFT JOIN SchedByGrade sc
  ON sc.gradeLevel = a.gradeLevel
  OR (sc.gradeLevel IS NULL AND a.gradeLevel IS NULL)
-- 9th, 10th, 11th, 12th, then Total.
ORDER BY IF(a.gradeLevel IS NULL, 1, 0), a.gradeLevel
