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
    "lde_site_plan_revisions": """
    SELECT
        f.FOLDERTYPE,
        f.FOLDERREVISION,
        f.FOLDERRSN,
        f.SUBCODE,
        sub.SUBDESC,
        vs.STATUSDESC,
        f.FOLDERCONDITION,
        f.REFERENCEFILE,
        f.FOLDERNAME,
        vu.USERNAME AS REVIEWER,
        fp.PROCESSRSN,
        vp.PROCESSDESC AS PROCESS_NAME,
        TO_CHAR(fp.STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as START_DATE,
        TO_CHAR(fp.ENDDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as END_DATE,
        TO_CHAR(fp.SCHEDULEDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as TO_START,
        TO_CHAR(fp.SCHEDULEENDDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as TO_END,
        vps.STATUSDESC PROCESS_STATUS,
        ROW_NUMBER() OVER (PARTITION BY f.FOLDERRSN,
            fp.PROCESSCODE ORDER BY f.FOLDERRSN,
            fp.PROCESSCODE) cyclenumber,
        pi.PROPINFOVALUE CouncilDistrict
    FROM
        folder f
        JOIN folderprocess fp ON fp.FOLDERRSN = f.FOLDERRSN
            AND fp.PROCESSCODE IN(51212)
        JOIN validprocess vp ON vp.PROCESSCODE = fp.PROCESSCODE
        LEFT JOIN validuser vu ON vu.USERID = fp.ASSIGNEDUSER
        JOIN validprocessstatus vps ON vps.STATUSCODE = fp.STATUSCODE
        JOIN propertyinfo pi ON pi.PROPERTYRSN = f.PROPERTYRSN
            AND pi.PROPERTYINFOCODE = 52026 --Propertyinfo-Council District
        left outer JOIN VALIDSUB sub on sub.SUBCODE = f.SUBCODE
        left outer JOIN VALIDSTATUS vs on vs.STATUSCODE = f.statuscode
    GROUP BY
        f.FOLDERTYPE,
        f.FOLDERREVISION,
        vp.PROCESSDESC,
        f.FOLDERRSN,
        f.REFERENCEFILE,
        f.FOLDERNAME,
        fp.PROCESSCODE,
        fp.PROCESSRSN,
        vu.USERNAME,
        fp.SCHEDULEDATE,
        fp.SCHEDULEENDDATE,
        fp.STARTDATE,
        fp.ENDDATE,
        vps.STATUSDESC,
        pi.PROPINFOVALUE,
        f.SUBCODE,
        sub.SUBDESC,
        vs.STATUSDESC,
        f.FOLDERCONDITION
    ORDER BY
        vu.USERNAME,
        f.FOLDERTYPE,
        vp.PROCESSDESC,
        fp.PROCESSRSN
    """,
    "row_inspector_permit_list": """
    SELECT vs.subdesc                                      AS PERMIT_TYPE,
           f.foldertype                                    AS FOLDERTYPE,
           f.referencefile                                 AS PERMIT,
           f.folderrsn                                     AS FOLDERRSN,
           f.foldername                                    AS FOLDER_NAME,
           pr.propertyname                                 AS PROPERTY_NAME,
           f.expirydate                                    AS EXPIRY_DATE,
           f.issuedate                                     AS ISSUE_DATE,
           p.organizationname                              AS CONTRACTOR,
           p.phone1                                        AS PHONE,
           vw.workdesc                                     as RW_WORK_DESCRIPTION,
           trunc(trunc(f.expirydate) - trunc(f.issuedate)) AS WZ_Duration,          -- used for DS permits
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75390)                        Total_Days,           -- only for RW permits
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75980)                        Event_Start_Date,     -- only for RW permits
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 76110)                        Start_Date,           -- EX permits only
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75993)                        Extension_Start_Date, -- EX permits only
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75994)                        Extension_End_Date,   -- EX permits only
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 76115)                        End_Date,             -- EX permits only
           (SELECT count(processrsn) as count
            FROM folderprocessdeficiency fprd
            WHERE fprd.PROCESSRSN = fpr.PROCESSRSN
              AND fprd.statuscode = 55360)                    Count_deficiencies,
           (SELECT max(attemptdate) as inspection_date
            FROM AMANDA.FOLDERPROCESSATTEMPT fpra
            WHERE fpra.PROCESSRSN = fpr.PROCESSRSN)           Most_Recent_Inspection
    FROM folder f
             left JOIN validsub vs on vs.subcode = f.subcode
             left JOIN VALIDWORK vw on vw.workcode = f.workcode
             left JOIN property pr on pr.propertyrsn = f.propertyrsn
             left JOIN folderpeople fp on fp.folderrsn = f.folderrsn
             left JOIN people p on p.peoplersn = fp.peoplersn
             left join (select processrsn, folderrsn from FOLDERPROCESS where processcode = 50685) fpr
                       on fpr.FOLDERRSN = f.FOLDERRSN
    WHERE f.PRIORITY != 1
      AND (
        (f.foldertype = 'RW'
             AND f.subcode = 50500 --TURP only
             AND f.foldername NOT LIKE 'LA-%' -- removing LAs'
             AND fp.peoplecode = 1
             AND f.statuscode = 50010 --ACTIVE Permits only)
            OR (f.foldertype in ('EX', 'DS')
                AND fp.peoplecode = 50065 -- ROW Contractors
                AND f.statuscode = 50010))) --ACTIVE Permits only
    """,
    "row_inspector_segment_list": """
    SELECT
        fp.folderrsn AS FOLDERRSN,
        fp.propertyrsn AS PROPERTYRSN,
        CASE WHEN fp.PROPERTYRELATIONCODE = 5 THEN
            'TRUE'
        ELSE
            'FALSE'
        END AS is_primary
    FROM
        folder f,
        folderproperty fp,
        property p
    WHERE (f.foldertype = 'RW'
        AND f.subcode = 50500 --TURP
        AND f.statuscode = 50010 --ACTIVE
        AND f.foldername NOT LIKE 'LA-%'
        AND p.propcode = 52010
        AND f.folderrsn = fp.folderrsn
        AND fp.propertyrsn = p.propertyrsn)
        OR(f.foldertype in('EX', 'DS')
            AND f.statuscode = 50010 --ACTIVE
            AND p.propcode = 52010
            AND f.folderrsn = fp.folderrsn
            AND fp.propertyrsn = p.propertyrsn)
    ORDER BY
        fp.folderrsn
    """,
    "tds_cases": """
    SELECT
        foldertype,
        folderrsn,
        referencefile,
        foldername,
        Reviewer,
        processrsn,
        processname,
        tostart_date,
        duedate,
        started_date,
        ended_date,
        process_status,
        CAST(cyclenumber AS NUMBER(10, 0)) CycleNumber,
        CouncilDistrict
    FROM
        (
            SELECT
                f.foldertype,
                f.folderrsn,
                f.referencefile,
                f.foldername,
                vu.username Reviewer,
                fp.processrsn,
                vp.PROCESSDESC ProcessName,
                TO_CHAR(fp.scheduledate, 'YYYY-MM-DD"T"HH24:MI:SS') ToStart_Date,
                TO_CHAR(fp.scheduleEndDate, 'YYYY-MM-DD"T"HH24:MI:SS') DueDate,
                TO_CHAR(fp.startdate, 'YYYY-MM-DD"T"HH24:MI:SS') Started_Date,
                TO_CHAR(fp.enddate, 'YYYY-MM-DD"T"HH24:MI:SS') Ended_Date,
                vps.statusdesc process_status,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        f.folderrsn,
                        fp.processcode
                    ORDER BY
                        f.folderrsn,
                        fp.processcode
                ) cyclenumber,
                pi.propinfovalue CouncilDistrict
            FROM
                folder f
                JOIN folderprocess fp ON fp.folderrsn = f.folderrsn
                AND fp.processcode IN (51132, 51834, 84102)
                JOIN validprocess vp ON vp.processcode = fp.processcode
                LEFT JOIN validuser vu ON vu.userid = fp.assigneduser
                JOIN validprocessstatus vps ON vps.statuscode = fp.statuscode
                JOIN propertyinfo pi ON pi.propertyrsn = f.propertyrsn
                AND pi.propertyinfocode = 52026 --Propertyinfo-Council District
            WHERE
                f.foldertype IN ('C8', 'SP', 'ZC', 'PR', 'CC')
            GROUP BY
                f.foldertype,
                vp.PROCESSDESC,
                f.folderrsn,
                f.referencefile,
                f.foldername,
                fp.processcode,
                fp.processrsn,
                vu.username,
                fp.scheduledate,
                fp.scheduleEndDate,
                fp.startdate,
                fp.enddate,
                vps.statusdesc,
                pi.propinfovalue
            ORDER BY
                vu.username,
                f.foldertype,
                vp.processdesc,
                fp.processrsn
        )
    """,
    "tds_asmd_map":
    """
    SELECT
        f.folderrsn,
        f.parentrsn,
        p.PropHouse || ' ' || p.PropStreet || ' ' || p.PropStreetType || ' ' || p.PropStreetDirection || ' ' || p.PropUnitType || ' ' || p.PropUnit || ' ' || p.propprovince || ' ' || p.proppostal AS "Primary Folder Property Address",
        fp.propertyrsn AS "Primary Folder Property PROPID",
        p.propertyroll AS "Primary Folder Property Roll Number",
        PI.propinfovalue AS "Primary Folder Property SEGM_GIS_ID",
        vs.statusdesc,
        f.foldername,
        (
            SELECT
                sum(ab.totalpaid)
            FROM
                accountbill ab
            WHERE
                ab.folderrsn = f.folderrsn
        ) AS "Total amount fees Paid",
        FI1.infovalue AS "Offset Amount",
        FI2.infovalue AS "ROW Dedication Value Calculated",
        FI3.infovalue AS "Easement Dedication Value Calculated",
        (
            SELECT
                SUM(ff.F03)
            FROM
                folderfreeform ff
            WHERE
                ff.folderrsn = f.folderrsn
        ) AS "Total Affordability Units",
        FI4.infovalue AS "Council District",
        FI5.infovalue AS "SIF District Area",
        FI6.infovalue AS "Total Affordability Reduction",
        FI7.infovalue AS "Transportation Land Use"
    FROM
        folder f
        JOIN validstatus vs ON f.statuscode = vs.statuscode
        JOIN folderproperty fp ON f.folderrsn = fp.folderrsn
        AND f.propertyrsn = fp.propertyrsn
        JOIN property p ON p.propertyrsn = fp.propertyrsn
        JOIN propertyinfo PI ON p.propertyrsn = PI.propertyrsn
        AND PI.propertyinfocode = 55005
        JOIN FOLDERINFO FI1 ON F.FOLDERRSN = FI1.FOLDERRSN
        AND FI1.INFOCODE = 84021
        JOIN FOLDERINFO FI2 ON F.FOLDERRSN = FI2.FOLDERRSN
        AND FI2.INFOCODE = 84040
        JOIN FOLDERINFO FI3 ON F.FOLDERRSN = FI3.FOLDERRSN
        AND FI3.INFOCODE = 84043
        JOIN FOLDERINFO FI4 ON F.FOLDERRSN = FI4.FOLDERRSN
        AND FI4.INFOCODE = 84011
        JOIN FOLDERINFO FI5 ON F.FOLDERRSN = FI5.FOLDERRSN
        AND FI5.INFOCODE = 84012
        JOIN FOLDERINFO FI6 ON F.FOLDERRSN = FI6.FOLDERRSN
        AND FI6.INFOCODE = 84044
        JOIN FOLDERINFO FI7 ON F.FOLDERRSN = FI7.FOLDERRSN
        AND FI7.INFOCODE = 84016
    WHERE
        f.foldertype = 'SIF'
        AND f.statuscode != 70045
    ORDER BY
        f.folderrsn
    """
}
