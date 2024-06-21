QUERIES = {
    "applications_received": """
    SELECT
        Foldertype,
        subcode,
        TO_CHAR(ROUND(INDATE, 'DDD'), 'YYYY-MM-DD'),
        COUNT(1) IssuedROWPermits
    FROM
        folder
    WHERE (foldertype in('DS')
        AND STATUSCODE NOT IN(50005, 50003, 70045)
        AND INDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
        AND INDATE IS NOT NULL)
        OR(foldertype in('RW', 'EX')
            AND STATUSCODE NOT IN(70045, 50003)
            AND INDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
            AND SUBCODE NOT IN(50510, 50505)
            AND INDATE IS NOT NULL)
    GROUP BY
        TO_CHAR(ROUND(INDATE, 'DDD'), 'YYYY-MM-DD'),
        Foldertype,
        subcode
    ORDER BY
        Foldertype
    """,
    "active_permits": """
    SELECT
        Foldertype,
        COUNT(1) ACTIVEPERMITS
    FROM
        folder
    WHERE (foldertype in('EX', 'DS')
        AND STATUSCODE IN(50010))
        OR(foldertype in('RW')
            AND STATUSCODE IN(50010)
            AND FOLDERNAME NOT LIKE 'LA-%')
    GROUP BY
        Foldertype
    ORDER BY
        Foldertype
    """,
    "issued_permits": """
    SELECT
        Foldertype,
        subcode,
        TO_CHAR(ROUND(ISSUEDATE, 'DDD'), 'YYYY-MM-DD'),
        COUNT(1) IssuedROWPermits
    FROM
        folder
    WHERE (foldertype in('EX', 'DS')
        AND ISSUEDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
        AND ISSUEDATE IS NOT NULL)
        OR(foldertype in('RW')
            AND ISSUEDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
            AND SUBCODE NOT IN(50510, 50505)
            AND ISSUEDATE IS NOT NULL)
    GROUP BY
        TO_CHAR(ROUND(ISSUEDATE, 'DDD'), 'YYYY-MM-DD'),
        Foldertype,
        subcode
    ORDER BY
        Foldertype
    """,
    "review_time": """
    SELECT
        f.CUSTOMFOLDERNUMBER,
        f.FOLDERRSN,
        f.INDATE,
        f.ISSUEDATE,
        pa.STARTDATE AS WEBAPPSTART,
        pa.ENDDATE AS WEBAPPEND,
        pe.STARTDATE AS EXTEND,
        fa.ATTEMPTDATE AS DEPT_COMMENTS
    FROM
        FOLDER f
        LEFT OUTER JOIN FOLDERPROCESS pa ON f.FOLDERRSN = pa.FOLDERRSN
        AND pa.PROCESSCODE = 70000
        LEFT OUTER JOIN FOLDERPROCESS pe ON f.FOLDERRSN = pe.FOLDERRSN
        AND pe.PROCESSCODE = 50680
        LEFT OUTER JOIN FOLDERPROCESSATTEMPT fa ON f.FOLDERRSN = fa.FOLDERRSN
    WHERE
        f.FOLDERTYPE = 'RW'
        AND f.SUBCODE = 50500
        AND f.WORKCODE = 50590
        AND fa.RESULTCODE = 61510
    """,
    "ex_permits_issued": """
    SELECT
        CONCAT(CONCAT(f.FOLDERYEAR, '-'), f.FOLDERSEQUENCE) AS PERMIT_ID,
        f.SUBCODE,
        vs.SUBDESC,
        f.FOLDERNAME,
        TO_CHAR(f.INDATE,'MM-DD-YYYY HH24:MI:SS'),
        TO_CHAR(f.ISSUEDATE,'MM-DD-YYYY HH24:MI:SS')
    FROM
        FOLDER f
        LEFT OUTER JOIN VALIDSUB vs ON f.SUBCODE = vs.SUBCODE
    WHERE
        FOLDERTYPE in('EX')
        AND ISSUEDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
        AND ISSUEDATE IS NOT NULL
        AND PRIORITY = 3
	""",
    "license_agreements_timeline": """
    SELECT
        f.FOLDERRSN,
        f.REFERENCEFILE,
        sub.SUBDESC,
        vs.STATUSDESC,
        f.FOLDERCONDITION,
        TO_CHAR(f.INDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS INDATE,
        TO_CHAR(web_acceptance.ATTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS WEB_APP_ACCEPT_DATE,
        TO_CHAR(payment.ATTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS PAYMENT_COMPLETED_DATE,
        TO_CHAR(reviews.enddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS REVIEW_END_DATE,
        TO_CHAR(f.ISSUEDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS ISSUEDATE
    FROM
        FOLDER f
        LEFT OUTER JOIN (
        SELECT
            fp.FOLDERRSN,
            min(fpa.ATTEMPTDATE) AS ATTDATE -- Getting latest web acceptance
        FROM
            FOLDERPROCESS fp
            LEFT OUTER JOIN FOLDERPROCESSATTEMPT fpa ON fpa.PROCESSRSN = fp.PROCESSRSN
        WHERE
            fp.PROCESSCODE in(70000) -- Web Application acceptance process
            AND fpa.RESULTCODE in(52130) -- Only "Accepted" Attempts
        GROUP BY
            fp.FOLDERRSN) web_acceptance ON f.FOLDERRSN = web_acceptance.FOLDERRSN
        LEFT OUTER JOIN (
        SELECT
            fp.FOLDERRSN,
            min(fpa.ATTEMPTDATE) AS ATTDATE -- Getting latest completed distribution 
        FROM
            FOLDERPROCESS fp
            LEFT OUTER JOIN FOLDERPROCESSATTEMPT fpa ON fpa.PROCESSRSN = fp.PROCESSRSN
        WHERE
            fp.PROCESSCODE in(51070) -- Initial Distribution process
            AND fpa.RESULTCODE in(55000) -- Only "Completed" Attempts
        GROUP BY
            fp.FOLDERRSN) payment ON f.FOLDERRSN = payment.FOLDERRSN
        LEFT OUTER JOIN (
        SELECT
            FOLDERRSN,
            max(ENDDATE) AS ENDDATE -- Getting most recent completed review
        FROM
            FOLDERPROCESS
        WHERE
            DISCIPLINECODE in(50030) -- Discipline group is "Review"
        GROUP BY
            FOLDERRSN) reviews ON f.FOLDERRSN = reviews.FOLDERRSN
        left outer JOIN VALIDSUB sub on sub.SUBCODE = f.SUBCODE
        left outer JOIN VALIDSTATUS vs on vs.STATUSCODE = f.statuscode
    WHERE
        f.FOLDERTYPE in('LM') -- Land Management folder type only
        AND f.STATUSCODE NOT in(56050) -- Remove VOID status
    """,
}
